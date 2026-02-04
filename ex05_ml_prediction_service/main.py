import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error


def load_data():
    """
    Charge les données depuis le bucket MinIO.

    Returns
    -------
    pd.DataFrame
        Données brutes de taxi.
    """
    storage_options = {
        "key": "minio",
        "secret": "minio123",
        "client_kwargs": {"endpoint_url": "http://localhost:9000"}
    }
    path = "s3://nyc-processed/yellow_tripdata_2024-01_clean.parquet"
    return pd.read_parquet(path, storage_options=storage_options)


def preprocess_data(df):
    """
    Nettoie les données et extrait les caractéristiques (features).

    Parameters
    ----------
    df : pd.DataFrame
        Données brutes.

    Returns
    -------
    X : pd.DataFrame
        Variables explicatives.
    y : pd.Series
        Cible à prédire (total_amount).
    """
    # Nettoyage des valeurs manquantes
    cols_clean = ['total_amount', 'trip_distance', 'passenger_count']
    df = df.dropna(subset=cols_clean)

    # Feature engineering
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['hour'] = df['tpep_pickup_datetime'].dt.hour

    features = [
        'trip_distance', 'passenger_count', 'hour',
        'PULocationID', 'DOLocationID'
    ]
    return df[features], df['total_amount']


def train_model(X, y):
    """
    Entraîne le modèle de régression.

    Parameters
    ----------
    X : pd.DataFrame
        Features d'entraînement.
    y : pd.Series
        Cible.

    Returns
    -------
    model : GradientBoostingRegressor
        Le modèle entraîné.
    """
    model = GradientBoostingRegressor(
        n_estimators=100, max_depth=5, random_state=42
    )
    model.fit(X, y)
    return model


def make_prediction(model, data):
    """
    Réalise une prédiction (Inférence).

    Parameters
    ----------
    model : GradientBoostingRegressor
        Modèle entraîné.
    data : pd.DataFrame
        Une ou plusieurs lignes de données.

    Returns
    -------
    np.ndarray
        Prédiction(s) du prix.
    """
    return model.predict(data)


if __name__ == "__main__":
    # Pipeline principal
    raw_df = load_data()
    X, y = preprocess_data(raw_df)

    # Split des données (ligne coupée pour PEP 8)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    trained_model = train_model(X_train, y_train)

    # Évaluation
    predictions = make_prediction(trained_model, X_test)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    # Double espace avant le commentaire ci-dessous pour E261
    print(f"Modèle prêt ! RMSE obtenu : {rmse:.2f}")  # Objectif < 10
