import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING

import sqlalchemy as sa
from nacsos_data.util.priority.ml import training, workload_estimation, report

from nacsos_data.db.schemas import Priority
from nacsos_data.models.priority import PriorityModel
from nacsos_data.util.annotations.export import wide_export_table
from nacsos_data.util.errors import NotFoundError
from nacsos_data.util.priority.mask import get_inclusion_mask
from nacsos_data.util.priority.plots import (
    inclusion_curve,
    scope_inclusions,
    buscar_frontiers,
    score_distribution,
    buscar_workload,
    roc_auc
)

from nacsos_data.db import DatabaseEngineAsync

if TYPE_CHECKING:
    import pandas as pd
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401


def store_df(dest: Path, df: 'pd.DataFrame') -> None:
    import numpy as np
    with open(dest, 'w') as f:
        json.dump(df.replace({np.nan: None}).to_dict(orient='tight'), fp=f)


async def main(db_engine: DatabaseEngineAsync,
               priority_id: str,
               out_path: Path,
               logger: logging.Logger,
               buscar_batchsize: int = 100,
               buscar_recall_target: float = 0.95,
               buscar_bias: float = 1.,
               buscar_confidence_level: float = 0.95,
               data_table: str = 'data.arrow',
               tab_incl_stats: str = 'inclusion_statistics.json',
               fig_incl_stats: str = 'inclusion_statistics.png',
               fig_incl_curve: str = 'inclusion_curve.png',
               fig_buscar: str = 'buscar.png',
               fig_roc_auc: str = 'roc_auc.png',
               tab_roc_auc: str = 'roc_auc.json',
               tab_buscar_p: str = 'buscar_p.json',
               tab_buscar_r: str = 'buscar_recall.json',
               fig_buscar_est: str = 'buscar_est.png',
               tab_buscar_est: str = 'buscar_est.json',
               tab_predictions: str = 'predictions.csv',
               fig_hist: str = 'score_distribution.png',
               tab_est: str = 'workload_estimation.txt',
               tab_test_eval: str = 'report_test.json',
               tab_self_eval: str = 'report_self.json',
               fig_params: dict[str, Any] | None = None) -> None:
    import pandas as pd
    import numpy as np

    # -----------------------------------------------------------------------------------------
    # Setup
    # -----------------------------------------------------------------------------------------
    out_path.mkdir(exist_ok=True, parents=True)
    logger.info(f'Going to write persisted outputs to {out_path}')

    logger.info(f'Fetching priority settings for {priority_id}')
    async with db_engine.session() as session:  # type: AsyncSession
        _priority = await session.scalar(sa.select(Priority).where(Priority.priority_id == priority_id))

        if _priority is None:
            raise NotFoundError(f'Priority for {priority_id} not found')

        priority = PriorityModel(**_priority.__dict__)

        _priority.time_started = datetime.now()
        await session.commit()

    incl_rule = priority.incl_rule
    incl_field = priority.incl_field
    incl_pred_field = priority.incl_pred_field
    config = priority.config

    if not incl_rule or not incl_field or not incl_pred_field or not config:
        raise ValueError('Some data is missing!')

    # -----------------------------------------------------------------------------------------
    # Data table
    # -----------------------------------------------------------------------------------------
    logger.info('Fetching wide export table...')
    async with db_engine.session() as session:  # type: AsyncSession
        base_cols, label_cols, df = await wide_export_table(session=session,
                                                            scope_ids=priority.source_scopes,
                                                            limit=None,
                                                            project_id=priority.project_id,
                                                            nql_filter=priority.nql_parsed)

    logger.info(f'  -> retrieved table with shape={df.shape}')
    logger.info(f'     / base_cols={base_cols}')
    logger.info(f'     / label_cols={label_cols}')

    logger.info(f'Preparing inclusion rule "{incl_rule}"')
    incl = get_inclusion_mask(df=df, rule=incl_rule, label_cols=label_cols)
    logger.info(f'  -> found {(~incl.isna()).sum()} inclusion labels of which {incl.sum():,} are `true`')
    df[incl_field] = incl

    mask_seen = ~incl.isna()

    # -----------------------------------------------------------------------------------------
    # Pre-training plots and stats
    # -----------------------------------------------------------------------------------------
    logger.info('Creating the inclusion curve...')
    fig = inclusion_curve(df=df, key=incl_field, fig_params=fig_params)
    fig.savefig(out_path / fig_incl_curve, transparent=True, bbox_inches='tight')

    logger.info('Creating the incl/excl per scope barplot...')
    fig, stats = scope_inclusions(df=df, key=incl_field, fig_params=fig_params)
    fig.savefig(out_path / fig_incl_stats, transparent=True, bbox_inches='tight')
    store_df(out_path / tab_incl_stats, stats)

    logger.info('Creating BUSCAR frontier plot...')
    fig, buscar, frontier = buscar_frontiers(df=df,
                                             key=incl_field,
                                             fig_params=fig_params,
                                             batch_size=buscar_batchsize)
    fig.savefig(out_path / fig_buscar, transparent=True, bbox_inches='tight')
    store_df(out_path / tab_buscar_p, buscar)
    store_df(out_path / tab_buscar_r, frontier)

    # -----------------------------------------------------------------------------------------
    # Model training
    # -----------------------------------------------------------------------------------------
    logger.info('Proceeding with model training and predictions...')

    if config.conf == 'SCIBERT' or config.conf == 'CLIMBERT':
        df = training(df=df,
                      text='text',
                      source=incl_field,
                      target=incl_pred_field,
                      model_name=config.model,
                      max_len=config.max_len,
                      train_split=priority.train_split or 0.8,
                      n_epochs=config.n_epochs,
                      batch_size_predict=config.batch_size_predict,
                      batch_size_train=config.batch_size_train,
                      batch_size_eval=config.batch_size_eval,
                      warmup_steps=config.warmup_steps,
                      weight_decay=config.weight_decay,
                      logging_steps=config.logging_steps,
                      eval_strategy=config.eval_strategy,
                      eval_steps=config.eval_steps)
    elif config.conf == 'SVM':
        raise NotImplementedError()
    elif config.conf == 'REG':
        raise NotImplementedError()
    else:
        raise NotImplementedError()

    logger.info('Writing predictions and full data-table to file...')
    df.to_feather(out_path / data_table)

    df = pd.read_feather(out_path / data_table)
    # -----------------------------------------------------------------------------------------
    # Post-train stats and plots
    # -----------------------------------------------------------------------------------------
    logger.info('Workload estimation...')
    est = workload_estimation(df=df,
                              source=incl_field,
                              target=incl_pred_field,
                              recall_targets=None)
    with open(out_path / tab_est, 'w') as f:
        f.write(est)

    logger.info('Creating classification report...')
    test_eval, self_eval = report(df=df,
                                  source=incl_field,
                                  target=incl_pred_field)
    store_df(out_path / tab_test_eval, test_eval)
    store_df(out_path / tab_self_eval, self_eval)

    logger.info('Buscar remaining workload estimation...')
    fig, buscar = buscar_workload(df=df, source=incl_field, target=incl_pred_field,
                                  batch_size=buscar_batchsize, fig_params=fig_params,
                                  recall_target=buscar_recall_target, bias=buscar_bias,
                                  confidence_level=buscar_confidence_level)
    fig.savefig(out_path / fig_buscar_est, transparent=True, bbox_inches='tight')
    store_df(out_path / tab_buscar_est, buscar)
    buscar.replace({np.nan: None}).to_csv(out_path / tab_predictions, index=False)

    logger.info('Creating the ROC/AUC plot...')
    fig, stats = roc_auc(df=df, source=incl_field, target=incl_pred_field,
                         fig_params=fig_params)
    fig.savefig(out_path / fig_roc_auc, transparent=True, bbox_inches='tight')
    store_df(out_path / tab_roc_auc, stats)

    logger.info('Creating the prediction score distribution plot...')
    fig = score_distribution(df=df, source=incl_field, target=incl_pred_field,
                             fig_params=fig_params)
    fig.savefig(out_path / fig_hist, transparent=True, bbox_inches='tight')

    # -----------------------------------------------------------------------------------------
    # Post-processing
    # -----------------------------------------------------------------------------------------
    logger.info(f'Updating info on priority settings for {priority_id}')
    async with db_engine.session() as session:  # type: AsyncSession
        _priority = await session.scalar(sa.select(Priority).where(Priority.priority_id == priority_id))

        if _priority is None:
            raise NotFoundError(f'Priority for {priority_id} not found')

        srtd = df[~mask_seen].sort_values(f'{incl_pred_field}:0')

        _priority.time_ready = datetime.now()
        _priority.prioritised_ids = srtd['item_id'][:priority.n_predictions or 1000].tolist()
        await session.commit()

    logger.info('All done!')
