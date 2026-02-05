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
    # On lit le fichier CLEAN généré par Spark
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
    df = df.dropna(subset=cols_clean).copy()

    # Feature engineering (Utilisation des NOUVEAUX noms de colonnes)
    # 'tpep_pickup_datetime' est devenu 'pickup_datetime' dans le fichier clean
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['hour'] = df['pickup_datetime'].dt.hour

    # Sélection des features avec les bons noms
    # PULocationID -> pickup_location_id
    # DOLocationID -> dropoff_location_id
    features = [
        'trip_distance', 'passenger_count', 'hour',
        'pickup_location_id', 'dropoff_location_id'
    ]

    # On vérifie que les colonnes existent avant de retourner
    # (Sécurité au cas où Spark n'aurait pas tout renommé comme prévu)
    available_features = [f for f in features if f in df.columns]

    return df[available_features], df['total_amount']


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
    # On utilise moins d'estimateurs pour que ça tourne vite sur ton PC
    model = GradientBoostingRegressor(
        n_estimators=50, max_depth=5, random_state=42
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
    print("--- Démarrage du ML Service ---")

    # 1. Chargement
    print("Chargement des données depuis MinIO...")
    raw_df = load_data()
    print(f"Données chargées : {raw_df.shape}")

    # 2. Préparation
    print("Préparation des features...")
    X, y = preprocess_data(raw_df)

    # 3. Split des données
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 4. Entraînement
    print("Entraînement du modèle (Gradient Boosting)...")
    trained_model = train_model(X_train, y_train)

    # 5. Évaluation
    print("Évaluation du modèle...")
    predictions = make_prediction(trained_model, X_test)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))

    print("-" * 30)
    print(f" Modèle prêt ! RMSE obtenu : {rmse:.2f}")  # Objectif < 10
    print("-" * 30)