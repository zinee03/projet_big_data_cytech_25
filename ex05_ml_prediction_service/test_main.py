import numpy as np
import pandas as pd

from main import preprocess_data


def test_data_integrity():
    """
    Vérifie l'intégrité des données pour l'entraînement.
    """
    # Création d'un jeu de données de test
    # ATTENTION : On utilise ici les noms "CLEAN" (post-Spark)
    sample_df = pd.DataFrame({
        'total_amount': [10.5, np.nan, 20.0],
        'trip_distance': [1.2, 3.0, 5.5],
        'passenger_count': [1, 2, 1],
        'pickup_datetime': [              # <-- Ancien nom : tpep_pickup_datetime
            '2024-01-01 12:00:00',
            '2024-01-01 13:00:00',
            '2024-01-01 14:00:00'
        ],
        'pickup_location_id': [1, 2, 3],  # <-- Ancien nom : PULocationID
        'dropoff_location_id': [4, 5, 6]  # <-- Ancien nom : DOLocationID
    })

    X, y = preprocess_data(sample_df)

    # Vérification : la ligne avec NaN doit être supprimée
    assert len(X) == 2
    # Vérification : la colonne 'hour' doit avoir été créée
    assert 'hour' in X.columns
    # Vérification : la cible 'y' correspond au total_amount
    assert y.name == 'total_amount'


def test_inference_single_row():
    """
    Vérifie la capacité d'inférence sur une donnée isolée.
    """
    # Simulation d'une entrée d'inférence unique avec les noms CLEAN
    single_row = pd.DataFrame({
        'trip_distance': [2.5],
        'passenger_count': [1.0],
        'pickup_datetime': ['2024-01-02 15:30:00'],    # <-- Corrigé
        'pickup_location_id': [132],                   # <-- Corrigé
        'dropoff_location_id': [230],                  # <-- Corrigé
        'total_amount': [18.0]
    })

    X, y = preprocess_data(single_row)

    # Vérification de l'inférence
    assert len(X) == 1
    assert X['hour'].iloc[0] == 15
    assert isinstance(X, pd.DataFrame)