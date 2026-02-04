-- ==============================================================
-- NETTOYAGE (On repart de zéro)
-- ==============================================================
DROP TABLE IF EXISTS Fact_Trips CASCADE;
DROP TABLE IF EXISTS Dim_Vendor CASCADE;
DROP TABLE IF EXISTS Dim_RateCode CASCADE;
DROP TABLE IF EXISTS Dim_PaymentType CASCADE;
DROP TABLE IF EXISTS Dim_Location CASCADE;
DROP TABLE IF EXISTS Dim_Time CASCADE;

-- ==============================================================
-- CRÉATION DES DIMENSIONS (Vides pour l'instant)
-- ==============================================================

CREATE TABLE Dim_Vendor (
                            vendor_id INT PRIMARY KEY,
                            vendor_name VARCHAR(50)
);

CREATE TABLE Dim_RateCode (
                              rate_code_id INT PRIMARY KEY,
                              rate_description VARCHAR(100)
);

CREATE TABLE Dim_PaymentType (
                                 payment_type_id INT PRIMARY KEY,
                                 payment_description VARCHAR(50)
);

CREATE TABLE Dim_Location (
                              location_id INT PRIMARY KEY,
                              borough VARCHAR(100),
                              zone VARCHAR(100),
                              service_zone VARCHAR(100)
);

CREATE TABLE Dim_Time (
                          pickup_datetime TIMESTAMP PRIMARY KEY,
                          year INT,
                          month INT,
                          day INT,
                          hour INT,
                          week_day VARCHAR(20),
                          is_weekend BOOLEAN
);

-- ==============================================================
-- TABLE DE FAIT : Fact_Trips
-- ==============================================================
-- Modifiée pour accepter les données sans vérifier les clés étrangères immédiatement
-- Ajout de 'dropoff_datetime' qui manquait

CREATE TABLE Fact_Trips (
                            trip_id SERIAL PRIMARY KEY,

    -- Clés vers les dimensions (Sans "REFERENCES" pour éviter les blocages à l'ingestion)
                            vendor_id INT,
                            rate_code_id INT,
                            payment_type_id INT,
                            pickup_location_id INT,
                            dropoff_location_id INT,

    -- Dates (Spark envoie les deux)
                            pickup_datetime TIMESTAMP,
                            dropoff_datetime TIMESTAMP, -- <--- CELLE-CI MANQUAIT !

    -- Métriques
                            passenger_count INT,
                            trip_distance DECIMAL(10, 2),
                            fare_amount DECIMAL(10, 2),
                            extra DECIMAL(10, 2),
                            mta_tax DECIMAL(10, 2),
                            tip_amount DECIMAL(10, 2),
                            tolls_amount DECIMAL(10, 2),
                            improvement_surcharge DECIMAL(10, 2),
                            total_amount DECIMAL(10, 2),
                            congestion_surcharge DECIMAL(10, 2)
);

-- Index pour la performance
CREATE INDEX idx_vendor ON Fact_Trips(vendor_id);
CREATE INDEX idx_pickup_loc ON Fact_Trips(pickup_location_id);
CREATE INDEX idx_date ON Fact_Trips(pickup_datetime);