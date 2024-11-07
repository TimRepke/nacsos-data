import logging
import tempfile
from typing import TYPE_CHECKING
import numpy as np

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger('nacsos_data.util.priority.labels')


def compute_metrics(p) -> dict[str, np.ndarray]:
    import evaluate
    logits, labels = p
    predictions = np.argmax(logits, axis=-1)
    return {
        'recall': evaluate.load('recall').compute(predictions=predictions,
                                                  references=labels,
                                                  zero_division=0,
                                                  average='weighted')['recall'],
        'precision': evaluate.load('precision').compute(predictions=predictions,
                                                        references=labels,
                                                        zero_division=0,
                                                        average='weighted')['precision'],
        'f1': evaluate.load('f1').compute(predictions=predictions,
                                          references=labels,
                                          labels=np.arange(len(labels)),
                                          average='weighted')['f1'],
        'accuracy': evaluate.load('accuracy').compute(predictions=predictions,
                                                      references=labels,
                                                      normalize=False)['accuracy']
    }


def training(df: 'pd.DataFrame',
             text: str = 'text',
             source: str = 'incl',
             target: str = 'pred|incl',
             model_name: str = 'climatebert/distilroberta-base-climate-f',
             max_len: int = 512,
             train_split: float = 0.9,
             n_epochs: int = 3,
             batch_size_predict: int = 50,
             batch_size_train: int = 16,
             batch_size_eval: int = 50,
             warmup_steps: int = 400,
             weight_decay: float = 0.01,
             logging_steps: int = 10,
             eval_strategy='steps',
             eval_steps: int = 50) -> 'pd.DataFrame':
    import torch
    from tqdm import tqdm
    from datasets import Dataset
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments

    # Create a copy of labelled data so we don't mess up the global dataframe
    dfi = df[~df[source].isna()][[text, source]].copy()
    dfi['label'] = dfi[source]
    labels = list(dfi['label'].unique())

    # Prepare a subset for training
    df_train = dfi.sample(frac=train_split)
    mask_train = dfi.index.isin(df_train.index)
    mask_test = ~mask_train
    df_test = dfi[mask_test]

    logger.info(f'From {df.shape[0]:,} rows, using {dfi.shape[0]:,} labels of [{labels}]')
    logger.info(f'Training data has {df_train.shape[0]:,} entries / {df_test.shape[0]:,} for testing')
    logger.info(f'Training labels: {df_train['label'].value_counts()} / '
                f'Testing labels: {df_test['label'].value_counts()}')

    tokenizer = AutoTokenizer.from_pretrained(model_name, max_length=max_len, model_max_length=max_len)

    train_dataset = Dataset.from_pandas(df_train)
    train_dataset = train_dataset.map(lambda rows: tokenizer(rows[text], padding='max_length', truncation=True),
                                      batched=True)

    eval_dataset = Dataset.from_pandas(df_test)
    eval_dataset = eval_dataset.map(lambda rows: tokenizer(rows[text], padding='max_length', truncation=True),
                                    batched=True)

    logger.info('Loading model...')
    model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=len(labels))

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Define training arguments
        logger.info('Setting up training arguments...')
        training_args = TrainingArguments(
            output_dir=f'{tmp_dir}/model',
            logging_dir=f'{tmp_dir}/logs',
            num_train_epochs=n_epochs,
            per_device_train_batch_size=batch_size_train,
            per_device_eval_batch_size=batch_size_eval,
            warmup_steps=warmup_steps,
            weight_decay=weight_decay,
            logging_steps=logging_steps,
            eval_strategy=eval_strategy,
            eval_steps=eval_steps,
        )

        logger.info('Initialising trainer...')
        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            compute_metrics=compute_metrics,
        )

        # Train the model
        logger.info('Training model...')
        trainer.train()

        logger.info('Predicting...')
        predictions = []
        with torch.no_grad():
            ds = Dataset.from_pandas(df)
            ds = ds.map(lambda x: tokenizer(x[text], padding='max_length', truncation=True), batched=True)
            ds.set_format('torch')

            for batch in tqdm(ds.iter(batch_size=batch_size_predict)):
                pred = model(input_ids=batch['input_ids'].to('cuda'), attention_mask=batch['attention_mask'].to('cuda'))
                predictions.append(torch.softmax(pred.logits, dim=1).cpu())

        logger.info('Writing predictions to dataframe...')
        preds = torch.concatenate(predictions)

        df[target] = preds.argmax(dim=1)
        df[f'{target}:0'] = preds[:, 0]
        df[f'{target}:1'] = preds[:, 1]

        df.loc[df_train.index, f'{target}-train'] = 1
        df.loc[df_test.index, f'{target}-test'] = 1

    return df


def report(df: 'pd.DataFrame',
           source: str = 'incl',
           target: str = 'pred|incl') -> tuple['pd.DataFrame', 'pd.DataFrame']:
    from sklearn.metrics import classification_report

    logger.info('Computing classification report on test data...')
    y_true = df[df[f'{target}-test'] == 1][source].to_numpy().astype(int)
    y_pred = df[df[f'{target}-test'] == 1][[f'{target}:0', f'{target}:1']].to_numpy()
    test_eval = pd.DataFrame(classification_report(y_true, y_pred.argmax(axis=1),
                                                   output_dict=True, zero_division=True,
                                                   target_names=['Exclude', 'Include']))

    logger.info('Computing classification report on training data...')
    y_true = df[df[f'{target}-train'] == 1][source].to_numpy().astype(int)
    y_pred = df[df[f'{target}-train'] == 1][[f'{target}:0', f'{target}:1']].to_numpy()
    self_eval = pd.DataFrame(classification_report(y_true, y_pred.argmax(axis=1),
                                                   output_dict=True, zero_division=True,
                                                   target_names=['Exclude', 'Include']))
    return test_eval, self_eval


def workload_estimation(df: 'pd.DataFrame',
                        source: str = 'incl',
                        target: str = 'pred|incl',
                        recall_targets: list[float] | None = None) -> str:
    from sklearn.metrics import precision_recall_curve

    y_true = df[df[f'{target}-test'] == 1][source].to_numpy().astype(int)
    y_pred = df[df[f'{target}-test'] == 1][[f'{target}:0', f'{target}:1']].to_numpy()

    precision, recall, thresholds = precision_recall_curve(y_true, y_pred)

    ret = ''
    for TARGET_RECALL in (recall_targets or [0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.98]):
        ret += '=================================\n'
        ret += f'Stats for target recall of {TARGET_RECALL}\n'
        ret += '=================================\n'
        ret += '> Stats on test set\n'
        idx = np.argwhere(recall > TARGET_RECALL).max()
        ret += f'idx {idx}\n'
        ret += f'num test items: {len(y_pred)}\n'
        ret += f'threshold: {thresholds[idx]}\n'
        ret += f'precision: {precision[idx]}\n'
        ret += f'recall: {recall[idx]}\n'
        ret += f'num above threshold: {(y_pred >= thresholds[idx]).sum()}\n'
        ret += f'num below threshold: {(y_pred < thresholds[idx]).sum()}\n'
        ret += f'approx. false negative: {int(len(y_pred) * (1 - recall[idx]))}\n'
        ret += f'approx. false positive: {int((y_pred >= thresholds[idx]).sum() * (1 - precision[idx]))}\n'
        ret += '\n'
        ret += '> Extrapolation\n'

        mask_new = df['import_upd'] & ~df['import_orig']
        mask_th = df['pred_incl|1'] > thresholds[idx]
        n_incl = (mask_th & mask_new).sum()
        r = recall[idx]
        p = precision[idx]

        ret += f'Total documents: {df.shape[0]:,}\n'
        ret += f'Original: {df['import_orig'].sum():,}, new query: {df['import_upd'].sum():,}\n'
        ret += f'New query (excl orig): {mask_new.sum():,}\n'
        ret += f'Num documents above threshold: {mask_th.sum():,}, num new docs above threshold: {n_incl:,}\n'
        ret += f'Extrapolating false negatives based on test recall: {int(mask_new.sum() * (1 - r)):,}\n'
        ret += f'Extrapolating false positives based on test precision: {int(n_incl * (1 - p)):,}\n'
        ret += '\n'
        ret += '\n'
    return ret