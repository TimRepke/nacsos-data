from typing import Callable, Any, TypeAlias, AsyncGenerator

from pydantic import BaseModel
from sklearn.base import TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression, RidgeClassifier
from sklearn.ensemble import AdaBoostClassifier, RandomForestClassifier
from sklearn.naive_bayes import GaussianNB, MultinomialNB
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics import precision_recall_fscore_support
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.db.schemas import AcademicItem
from nacsos_data.util.annotations.label_transform import get_annotations, annotations_to_sequence

ModelType: TypeAlias = (
    AdaBoostClassifier | DecisionTreeClassifier | RandomForestClassifier | MultinomialNB | GaussianNB | LogisticRegression | RidgeClassifier | SVC
)
FeaturiserType: TypeAlias = TfidfVectorizer | TransformerMixin | Pipeline

Featurisers: dict[str, Callable[[], FeaturiserType]] = {
    'tfidf(ngrams=(1,1), df=(0.01, 0.95))': lambda: TfidfVectorizer(ngram_range=(1, 1), min_df=5, max_df=0.95),
    'tfidf(ngrams=(1,1), df=(0.01, 0.95)) + pca(50)': lambda: Pipeline(
        [('tfidf', TfidfVectorizer(ngram_range=(1, 1), min_df=5, max_df=0.95)), ('pca', TruncatedSVD(n_components=50))]
    ),
}

Models: dict[str, Callable[[], ModelType]] = {
    'AdaBoost(LogReg(balanced), n_est=100)': lambda: AdaBoostClassifier(estimator=LogisticRegression(class_weight='balanced'), n_estimators=100),
    'DecisionTree(balanced)': lambda: DecisionTreeClassifier(class_weight='balanced'),
    'RandomForest(balanced)': lambda: RandomForestClassifier(class_weight='balanced'),
    'NaiveBayesMult': lambda: MultinomialNB(),
    'NaiveBayesGauss': lambda: GaussianNB(),
    'LogReg(balanced)': lambda: LogisticRegression(class_weight='balanced'),
    'Ridge(balanced)': lambda: RidgeClassifier(class_weight='balanced'),
    'SVM(gamma=2,C=1,balanced)': lambda: SVC(gamma=2.0, C=1.0, probability=True, class_weight='balanced'),
    'SVM(gamma=2,balanced)': lambda: SVC(C=2.0, probability=True, class_weight='balanced'),
    'SVM(C=1,balanced)': lambda: SVC(C=1.0, probability=True, class_weight='balanced'),
    'SVM(gamma=0.25,balanced)': lambda: SVC(C=0.25, probability=True, class_weight='balanced'),
    'SVM(C=0.025,balanced)': lambda: SVC(C=0.025, probability=True, class_weight='balanced'),
}


class Scores(BaseModel):
    f1: float
    precision: float
    recall: float
    support: float | None


class THScores(Scores):
    threshold: float


# List of tuples of item_id and class (0=exclude, 1=include)
BinaryPredictions = list[tuple[str, int]]
# List of tuples of item_id, class (0=exclude, 1=include), and class score
ProbaPredictions = list[tuple[str, int, float]]
Predictions: TypeAlias = BinaryPredictions | ProbaPredictions


def train_model(texts: list[str], labels: list[int], model: str, features: str) -> tuple[FeaturiserType, ModelType]:
    if model not in Models:
        raise KeyError(f'Model configuration "{model}" unknown.')
    if features not in Featurisers:
        raise KeyError(f'Featuriser configuration "{features}" unknown.')

    pre = Featurisers[features]()
    clf = Models[model]()

    x_train = pre.fit_transform(texts)
    clf.fit(x_train, labels)

    return pre, clf


def test_model(
    pre: FeaturiserType, clf: ModelType, labels: list[int], texts: list[str] | None = None, x_test: Any | None = None, thresholds: list[float] | None = None
) -> tuple[Scores, list[THScores] | None]:
    if thresholds is None:
        thresholds = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    if texts is None and x_test is None:
        raise AttributeError('Need either texts or vectors!')
    if x_test is None and texts is not None:
        x_test = pre.fit_transform(texts)

    y_pred = clf.predict(x_test)
    prec, rec, f1, supp = precision_recall_fscore_support(labels, y_pred, average='binary', zero_division=0.0)
    scores = Scores(precision=prec, recall=rec, f1=f1, support=supp)

    th_scores: list[THScores] | None = None
    if hasattr(clf, 'predict_proba'):
        th_scores = []
        y_pred = clf.predict_proba(x_test)
        for th in thresholds:
            y_pred_bin = [int(b) for b in y_pred[:, 1] >= th]
            prec, rec, f1, supp = precision_recall_fscore_support(labels, y_pred_bin, average='binary', zero_division=0.0)
            th_scores.append(THScores(threshold=th, precision=prec, recall=rec, f1=f1, support=supp))
    return scores, th_scores


@ensure_session_async
async def get_predictions(
    session: DBSession,
    inclusion_rule: str,
    project_id: str,
    source_ids: list[str],
    model: str,
    features: str,
    majority_on_conflict: bool = True,
) -> tuple[Scores, list[THScores] | None, Predictions]:
    item_ids_seen, texts_seen, labels = await get_labelled_texts(
        session=session, inclusion_rule=inclusion_rule, source_ids=source_ids, majority_on_conflict=majority_on_conflict
    )

    pre, clf = train_model(texts=texts_seen, labels=labels, model=model, features=features)
    scores, th_scores = test_model(pre=pre, clf=clf, texts=texts_seen, labels=labels)

    predictions: Predictions
    predictions = []

    async for batch_ids, batch_texts in project_texts_batched(session=session, project_id=project_id, item_ids_skip=set(item_ids_seen)):
        X = pre.transform(batch_texts)
        if hasattr(clf, 'predict_proba'):
            y_sft = clf.predict_proba(X)
            y_bin = y_sft.argmax(axis=1)
            predictions += [
                (bid, int(yb), float(ys))  # type: ignore  # FIXME
                for ys, yb, bid in zip(y_sft, y_bin, batch_ids, strict=False)
            ]
        else:
            y_bin = clf.predict(X)
            predictions += [
                (bid, int(yb))  # type: ignore  # FIXME
                for yb, bid in zip(y_bin, batch_ids, strict=False)
            ]
    return scores, th_scores, predictions


@ensure_session_async
async def get_labelled_texts(
    session: DBSession, inclusion_rule: str, source_ids: list[str], majority_on_conflict: bool = True
) -> tuple[list[str], list[str], list[int]]:
    annotations = [anno for sid in source_ids for anno in await get_annotations(session=session, source_ids=[sid])]
    labels = annotations_to_sequence(inclusion_rule, annotations=annotations, majority=majority_on_conflict)

    item_ids = [str(anno.item_id) for anno in annotations]
    stmt = select(AcademicItem.item_id, AcademicItem.title, AcademicItem.text).where(AcademicItem.item_id.in_(item_ids))
    rslt = (await session.execute(stmt)).mappings().all()
    texts: list[str] = [(row['title'] or '') + ' ' + (row['text'] or '') for row in rslt]

    return item_ids, texts, labels


async def project_texts_batched(
    session: AsyncSession, project_id: str, batch_size: int = 500, item_ids_skip: set[str] | None = None
) -> AsyncGenerator[tuple[list[str], list[str]], None]:
    stmt = (select(AcademicItem.item_id, AcademicItem.title, AcademicItem.text).where(AcademicItem.project_id == project_id)).execution_options(
        yield_per=batch_size
    )
    rslt = (await session.stream(stmt)).mappings().partitions()
    async for batch in rslt:
        if item_ids_skip is not None:
            batch = [ai for ai in batch if str(ai['item_id']) not in item_ids_skip]
        texts = [(row['title'] or '') + ' ' + (row['text'] or '') for row in batch]
        batch_ids = [str(ai['item_id']) for ai in batch]
        yield batch_ids, texts


@ensure_session_async
async def compare_models(
    session: DBSession, inclusion_rule: str, source_ids: list[str], features: str, n_splits: int = 8, majority_on_conflict: bool = True
) -> dict[str, dict[str, list[tuple[Scores, list[THScores] | None]]]]:
    item_ids, texts, labels = await get_labelled_texts(
        session=session, inclusion_rule=inclusion_rule, source_ids=source_ids, majority_on_conflict=majority_on_conflict
    )

    kf = StratifiedKFold(n_splits=n_splits, random_state=None, shuffle=False)

    results: dict[str, dict[str, list[tuple[Scores, list[THScores] | None]]]] = {}
    for i, (train_index, test_index) in enumerate(kf.split(texts, labels)):
        print(f'=== FOLD {i + 1} ===')
        txt_train = [texts[ti] for ti in train_index]
        txt_test = [texts[ti] for ti in test_index]
        y_train = [labels[ti] for ti in train_index]
        y_test = [labels[ti] for ti in test_index]
        print(f'{sum(y_train):,}/{len(y_train):,} relevant in training and {sum(y_test):,}/{len(y_test):,} in testing')

        for pre_k in Featurisers.keys():
            print(f' --- {pre_k} ---')
            if pre_k not in results:
                results[pre_k] = {}
            pre = Featurisers[features]()

            print('  - Preprocessing training data...')
            x_train = pre.fit_transform(txt_train)
            print('  - vocab size:', len(pre[0].vocabulary_))

            print('  - Preprocessing test data...')
            x_test = pre.transform(txt_test)

            for clf_k in Models.keys():
                print(f'  - Fitting classifier {clf_k}')
                if clf_k not in results[pre_k]:
                    results[pre_k][clf_k] = []
                try:
                    clf = Models[clf_k]()
                    clf.fit(x_train, y_train)

                    scores, th_scores = test_model(pre=pre, clf=clf, x_test=x_test, labels=y_test)
                    results[pre_k][clf_k].append((scores, th_scores))
                except Exception:
                    pass
    return results
