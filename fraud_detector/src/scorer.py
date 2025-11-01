import pandas as pd
import logging
from catboost import CatBoostClassifier

# Настройка логгера
logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

# Import model
model = CatBoostClassifier()
model.load_model('./models/my_model.cbm')

# Define optimal threshold
logger.info('Pretrained model imported successfully...')

def make_pred(dt, source_info="kafka"):
    logger.info('Computing prediction probabilities...')
    model_th = 0.33
    logger.info(f'Making predictions with threshold {model_th} (optimal for f1)')

    submission = pd.DataFrame({
        'score':  model.predict_proba(dt)[:, 1],
        'fraud_flag': (model.predict_proba(dt)[:, 1] > model_th) * 1
    })
    logger.info(f'Prediction complete for data from {source_info}')

    return submission


def get_prediction_proba(dt):
    """
    Efficiently get prediction probabilities for positive class.
    """
    logger.info('Computing prediction probabilities...')
    return model.predict_proba(dt)[:, 1]


def make_pred_from_proba(proba, path_to_file):
    model_th = 0.33
    logger.info(f'Making predictions using CatBoostClassifier with threshold {model_th} (optimal for f1)')

    indices = pd.read_csv(path_to_file).index
    predictions = (proba > model_th).astype(int)
    submission = pd.DataFrame({
        'index': indices,
        'prediction': predictions
    })
    logger.info('Prediction complete for file: %s', path_to_file)
    return submission


def get_top_features(n=5):
    feature_importances = model.get_feature_importance()
    feature_names = model.feature_names_

    # Pair feature names with importances and sort by importance (descending)
    feature_imp_pairs = sorted(
        zip(feature_names, feature_importances),
        key=lambda x: x[1],
        reverse=True
    )

    # Take top n and convert to dictionary
    top_features = dict(feature_imp_pairs[:n])
    logger.info(f'Top {n} feature importances were extracted')
    return top_features