-- Insertion des données de référence (Dictionnaires officiels NYC TLC)

-- 1. Vendeurs
INSERT INTO Dim_Vendor (vendor_id, vendor_name) VALUES
                                                    (1, 'Creative Mobile Technologies, LLC'),
                                                    (2, 'VeriFone Inc.');

-- 2. Codes Tarif (Rate Codes)
INSERT INTO Dim_RateCode (rate_code_id, rate_description) VALUES
                                                              (1, 'Standard rate'),
                                                              (2, 'JFK'),
                                                              (3, 'Newark'),
                                                              (4, 'Nassau or Westchester'),
                                                              (5, 'Negotiated fare'),
                                                              (6, 'Group ride'),
                                                              (99, 'Unknown');

-- 3. Types de paiement
INSERT INTO Dim_PaymentType (payment_type_id, payment_description) VALUES
                                                                       (1, 'Credit card'),
                                                                       (2, 'Cash'),
                                                                       (3, 'No charge'),
                                                                       (4, 'Dispute'),
                                                                       (5, 'Unknown'),
                                                                       (6, 'Voided trip');

-- Note : Dim_Location et Dim_Time seront remplies dynamiquement par ton code Spark (Exo 2/3)
-- car elles contiennent trop de données pour être tapées à la main ici.