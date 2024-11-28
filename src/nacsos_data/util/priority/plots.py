from typing import TYPE_CHECKING, Any

from nacsos_data.util.annotations.evaluation.buscar import recall_frontier, retrospective_h0

if TYPE_CHECKING:
    import pandas as pd
    from matplotlib import pyplot as plt


def inclusion_curve(df: 'pd.DataFrame', key: str = 'incl',
                    fig_params: dict[str, Any] | None = None) -> 'plt.Figure':
    from matplotlib import pyplot as plt

    seen = ~df[key].isna()
    fig: plt.Figure
    ax: plt.Axes
    fig, ax = plt.subplots(figsize=(15, 5), **(fig_params or {}))

    ax.set_title('Actual annotations we did')
    ax.plot(df[seen]
            .sort_values(['scope_order', 'item_order'], ascending=True)[key]
            .cumsum(skipna=True)
            .reset_index(drop=True))
    ax.set_xlabel('Number of documents seen')
    ax.set_ylabel('Number of included documents')
    fig.tight_layout()
    return fig


def scope_inclusions(df: 'pd.DataFrame', key: str = 'incl',
                     fig_params: dict[str, Any] | None = None) -> tuple['plt.Figure', 'pd.DataFrame']:
    from matplotlib import pyplot as plt
    import pandas as pd
    seen = ~df[key].isna()

    n_items = df[seen].groupby('scope_order')['item_id'].count()
    n_incl = df[seen].groupby('scope_order')[key].sum()

    df = pd.DataFrame({
        'n_items': n_items,
        'n_incl': n_incl,
        'n_excl': n_items - n_incl,
        'p_incl': (n_incl / n_items) * 100,
        'p_excl': ((n_items - n_incl) / n_items) * 100,
    })

    axes: tuple[plt.Axes, plt.Axes]
    fig, axes = plt.subplots(1, 2, figsize=(15, 5), **(fig_params or {}))
    ax = axes[0]
    ax.set_title('Real (absolute)')
    ax.bar(df.index, df['n_incl'], label='include', color='green')
    ax.bar(df.index, -df['n_excl'], label='exclude', color='red')
    ax.legend(loc='lower left')
    ax = ax.twinx()
    ax.plot(n_items, label='Annotations per scope')
    ax.legend()

    ax = axes[1]
    ax.set_title('Real (relative)')
    ax.set_ylim(-100, 100)
    ax.bar(df.index, df['p_incl'], label='include', color='green')
    ax.bar(df.index, -df['p_excl'], label='exclude', color='red')
    ax.legend(loc='lower left')
    ax = ax.twinx()
    ax.plot(n_items, label='Annotations per scope')
    ax.legend()
    fig.tight_layout()

    return fig, df


def buscar_frontiers(df: 'pd.DataFrame',
                     key: str = 'incl',
                     batch_size: int = 100,
                     recall_target: float = 0.95,
                     bias: float = 1.,
                     confidence_level: float = 0.95,
                     fig_params: dict[str, Any] | None = None) -> tuple['plt.Figure', 'pd.DataFrame', 'pd.DataFrame']:
    from matplotlib import pyplot as plt
    import pandas as pd

    # Initialise a figure with two panels
    ax1: plt.Axes
    ax2: plt.Axes
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5), **(fig_params or {}))

    # Sort in order of prioritisation

    # Derive meta-data
    seen = ~df[key].isna()  # mask of seen documents
    n_seen = seen.sum()
    n_total = df.shape[0]

    # Compute H0
    buscar = retrospective_h0(df[seen][key], n_total,
                              batch_size=batch_size,
                              recall_target=recall_target,
                              bias=bias,
                              confidence_level=confidence_level)
    # Compute frontier
    recall = recall_frontier(df[key].dropna(), df.shape[0], bias=bias)

    # Produce left panel
    ax1.set_ylabel('Relevant documents found')
    ax1.plot(df[seen][key].cumsum())  # cumulative plot of included documents
    ax1.axvline(n_seen)  # vertical line for where we are with screening
    ax1.grid(axis='x')
    ax1.set_xlim(xmax=n_seen)

    ax1t = ax1.twinx()
    ax1t.scatter(buscar[0], buscar[1])  # type: ignore[arg-type]
    ax1t.set_ylim(ymax=1, ymin=0)
    ax1t.set_ylabel('p score')
    ax1t.grid()

    ax2.plot(recall[0], recall[1], marker='o')  # type: ignore[arg-type]
    ax2.set_ylabel('p score')
    ax2.set_xlabel('recall target')
    ax2.set_yticks([0.01, 0.05, 0.1, 0.25, 0.33, 0.5])
    ax2.grid()

    fig.tight_layout()
    return (fig,
            pd.DataFrame({'batch_size': buscar[0], 'p': buscar[1]}),
            pd.DataFrame({'recall': recall[0], 'p': recall[1]}))


def buscar_workload(df: 'pd.DataFrame',  # Assuming df is ordered by (scope_order, item_order)!
                    source: str = 'incl',
                    target: str = 'pred|incl',
                    batch_size: int = 100,
                    recall_target: float = 0.95,
                    bias: float = 1.,
                    confidence_level: float = 0.95,
                    fig_params: dict[str, Any] | None = None) -> tuple['plt.Figure', 'pd.DataFrame']:
    from matplotlib import pyplot as plt
    import pandas as pd
    import numpy as np

    if f'{target}:0' not in df.columns:
        raise ValueError('Expected column with prediction scores!')

    # Initialise a figure with two panels
    ax1: plt.Axes
    ax2: plt.Axes
    fig, ax1 = plt.subplots(1, 1, figsize=(10, 8), **(fig_params or {}))
    ax2 = ax1.twinx()

    # get baseline stats
    mask_seen = df[source].notna()
    mask_unseen = df[source].isna()
    num_total = df.shape[0]
    num_seen = mask_seen.sum()

    # Get a copy of the dataframe so we can manipulate it without side effects
    data = df[[source, f'{target}:1']].copy().rename(columns={source: 'incl', f'{target}:1': 'prediction'})

    # Create random incl/excl annotations biased by prediction scores
    data['oracle'] = np.random.binomial(1, data['prediction'], num_total)
    # Create coalesce column that always has a label (preferred human)
    data['incl'] = data['incl'].astype('Int8')
    data['label'] = data['incl'].combine_first(data['oracle'])
    data['seen'] = data['incl'].notna().astype(int)

    # Order dataframe by human labels first and unseen items sorted by classification score, highest first
    data = (pd
            .concat([data[mask_seen],
                     data.loc[data[mask_unseen].sort_values('prediction', ascending=False).index]])
            .reset_index()
            .rename(columns={'index': 'orig_order'}))

    # Compute H0
    buscar = retrospective_h0(data['label'],
                              num_total,
                              batch_size=batch_size,
                              recall_target=recall_target,
                              bias=bias,
                              confidence_level=confidence_level)
    # Add buscar score to table
    data.loc[buscar[0][:-1], 'buscar'] = buscar[1][:-1]

    # Add cumulative sum
    data['n_incl'] = data['label'].cumsum()

    ax1.set_ylabel('relevant documents identified')
    # Plot of included items
    ax1.plot(data[data['seen'] == 1]['n_incl'])
    # Vertical separator where screening is now
    ax1.axvline(num_seen)
    # Plot of predicted included items
    ax1.plot(data[data['seen'] == 0]['n_incl'], color='C0', ls='--')

    ax2.set_ylabel('p score')
    ax2.scatter(data['buscar'].index, data['buscar'])  # type: ignore[arg-type]
    ax2.set_ylim(ymin=0, ymax=1)
    ax2.grid()

    fig.tight_layout()

    return fig, data


def roc_auc(df: 'pd.DataFrame',
            source: str = 'incl',
            target: str = 'pred|incl',
            fig_params: dict[str, Any] | None = None) -> tuple['plt.Figure', 'pd.DataFrame']:
    import numpy as np
    import pandas as pd
    from matplotlib import pyplot as plt
    from sklearn.metrics import RocCurveDisplay, roc_curve, precision_recall_curve, PrecisionRecallDisplay

    y_true = df[df[f'{target}-test'] == 1][source].to_numpy().astype(int)
    y_pred = df[df[f'{target}-test'] == 1][[f'{target}:0', f'{target}:1']].to_numpy().argmax(axis=1)

    fpr, tpr, _ = roc_curve(y_true, y_pred)
    precision, recall, thresholds = precision_recall_curve(y_true, y_pred)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 8), **(fig_params or {}))

    roc_display = RocCurveDisplay(fpr=fpr, tpr=tpr)
    pr_display = PrecisionRecallDisplay(precision=precision, recall=recall)

    roc_display.plot(ax=ax1)
    pr_display.plot(ax=ax2)
    ax1.set_xticks(np.arange(0, 1.1, 0.1))
    ax1.set_yticks(np.arange(0, 1.1, 0.1))
    ax1.grid(visible=True, linestyle='--')
    ax2.set_xticks(np.arange(0, 1.1, 0.1))
    ax2.set_yticks(np.arange(0, 1.1, 0.1))
    ax2.grid(visible=True, linestyle='--')
    fig.tight_layout()

    return fig, pd.DataFrame({'precision': precision, 'recall': recall, 'threshold': thresholds.tolist() + [1]})


def score_distribution(df: 'pd.DataFrame',
                       source: str = 'incl',
                       target: str = 'pred|incl',
                       fig_params: dict[str, Any] | None = None) -> 'plt.Figure':
    import numpy as np
    from matplotlib import pyplot as plt

    fig: plt.Figure
    axes: list[list[plt.Axes]]
    fig, axes = plt.subplots(2, 3, figsize=(12, 3), **(fig_params or {}))

    ax = axes[0][0]
    ax.set_title('Sorted classifier scores for unseen items')
    y = np.array(sorted(df[df[source].isna()][f'{target}:1'], reverse=True))
    ax.plot(y)
    ax.vlines(np.argwhere(y > 0.5).max() if (y > 0.5).sum() > 0 else 1, 0, 1, colors='green', ls=':', lw=1)

    ax = axes[0][1]
    ax.set_title('Sorted classifier scores for annotated items')
    y = np.array(sorted(df[~df[source].isna()][f'{target}:1'], reverse=True))
    ax.plot(y)
    ax.vlines(np.argwhere(y > 0.5).max() if (y > 0.5).sum() > 0 else 1, 0, 1, colors='green', ls=':', lw=1)

    ax = axes[0][2]
    ax.set_title('Sorted classifier scores for items labelled as include')
    y = np.array(sorted(df[df[source] == 1][f'{target}:1'], reverse=True))
    ax.plot(y)
    ax.vlines(np.argwhere(y > 0.5).max() if (y > 0.5).sum() > 0 else 1, 0, 1, colors='green', ls=':', lw=1)

    ax = axes[1][0]
    ax.set_title('Histogram of classifier scores in test set')
    df[df[f'{target}-test'] == 1][f'{target}:1'].hist(bins=50, ax=ax)
    ax.set_xlim(0, 1)

    ax = axes[1][1]
    ax.set_title('Histogram of classifier scores in train set')
    df[df[f'{target}-train'] == 1][f'{target}:1'].hist(bins=50, ax=ax)
    ax.set_xlim(0, 1)

    ax = axes[1][2]
    ax.set_title('Histogram of classifier scores in unseen set')
    df[df[source].isna()][f'{target}:1'].hist(bins=50, ax=ax)
    ax.set_xlim(0, 1)

    fig.tight_layout()

    return fig
