-- Nettoyage au cas où on relance le script
DROP TABLE IF EXISTS Fact_Trips CASCADE;
DROP TABLE IF EXISTS Dim_Vendor CASCADE;
DROP TABLE IF EXISTS Dim_RateCode CASCADE;
DROP TABLE IF EXISTS Dim_PaymentType CASCADE;
DROP TABLE IF EXISTS Dim_Location CASCADE;
DROP TABLE IF EXISTS Dim_Time CASCADE;

-- 1. Dimension : Vendeur (La société qui fournit le compteur)
CREATE TABLE Dim_Vendor (
                            vendor_id INT PRIMARY KEY,
                            vendor_name VARCHAR(50)
);

-- 2. Dimension : Code Tarif (Standard, JFK, etc.)
CREATE TABLE Dim_RateCode (
                              rate_code_id INT PRIMARY KEY,
                              rate_description VARCHAR(100)
);

-- 3. Dimension : Type de paiement
CREATE TABLE Dim_PaymentType (
                                 payment_type_id INT PRIMARY KEY,
                                 payment_description VARCHAR(50)
);

-- 4. Dimension : Lieu (Basé sur les zones NYC)
-- Cette table sera remplie plus tard via Spark ou un import CSV
CREATE TABLE Dim_Location (
                              location_id INT PRIMARY KEY,
                              borough VARCHAR(100), -- Arrondissement (ex: Manhattan)
                              zone VARCHAR(100),    -- Quartier
                              service_zone VARCHAR(100)
);

-- 5. Dimension : Temps (Pour l'analyse temporelle)
CREATE TABLE Dim_Time (
                          pickup_datetime TIMESTAMP PRIMARY KEY,
                          year INT,
                          month INT,
                          day INT,
                          hour INT,
                          week_day VARCHAR(20),
                          is_weekend BOOLEAN
);

-- 6. Table de Fait : Les Courses (Fact Table)
-- Elle relie tout le monde et contient les chiffres
CREATE TABLE Fact_Trips (
                            trip_id SERIAL PRIMARY KEY,

    -- Clés étrangères vers les dimensions
                            vendor_id INT REFERENCES Dim_Vendor(vendor_id),
                            rate_code_id INT REFERENCES Dim_RateCode(rate_code_id),
                            payment_type_id INT REFERENCES Dim_PaymentType(payment_type_id),
                            pickup_location_id INT REFERENCES Dim_Location(location_id),
                            dropoff_location_id INT REFERENCES Dim_Location(location_id),
                            pickup_datetime TIMESTAMP REFERENCES Dim_Time(pickup_datetime),

    -- Métriques (Ce qu'on analyse)
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

-- Index pour accélérer les recherches (Bonus performance)
CREATE INDEX idx_vendor ON Fact_Trips(vendor_id);
CREATE INDEX idx_pickup_loc ON Fact_Trips(pickup_location_id);
CREATE INDEX idx_date ON Fact_Trips(pickup_datetime);