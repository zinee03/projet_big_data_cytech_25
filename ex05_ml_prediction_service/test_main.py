import pandas as pd
import pytest
import numpy as np
from main import preprocess_data


def test_data_integrity():
    """
    Vérifie l'intégrité des données pour l'entraînement.

    Ce test s'assure que les colonnes nécessaires sont présentes et
    que le nettoyage des valeurs nulles fonctionne.
    """
    # Création d'un jeu de données de test avec un NaN
    sample_df = pd.DataFrame({
        'total_amount': [10.5, np.nan, 20.0],
        'trip_distance': [1.2, 3.0, 5.5],
        'passenger_count': [1, 2, 1],
        'tpep_pickup_datetime': [
            '2024-01-01 12:00:00',
            '2024-01-01 13:00:00',
            '2024-01-01 14:00:00'
        ],
        'PULocationID': [1, 2, 3],
        'DOLocationID': [4, 5, 6]
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

    Ce test simule l'arrivée d'une seule ligne de données (une course)
    pour vérifier que le pipeline de prétraitement ne plante pas.
    """
    # Simulation d'une entrée d'inférence unique
    single_row = pd.DataFrame({
        'trip_distance': [2.5],
        'passenger_count': [1.0],
        'tpep_pickup_datetime': ['2024-01-02 15:30:00'],
        'PULocationID': [132],
        'DOLocationID': [230],
        'total_amount': [18.0]  # Inclus car requis par preprocess_data
    })

    X, y = preprocess_data(single_row)

    # Vérification de l'inférence
    assert len(X) == 1
    assert X['hour'].iloc[0] == 15
    assert isinstance(X, pd.DataFrame)