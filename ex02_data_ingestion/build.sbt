ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex02_data_ingestion"
  )

// Dépendances Spark
// ATTENTION : "provided" signifie qu'IntelliJ ne les chargera pas par défaut.
// Pense à cocher "Include dependencies with Provided scope" dans ta config de Run [cite: 23, 24]
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"

// Dépendances MinIO (S3)
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"

// --- AJOUT OBLIGATOIRE POUR L'EXERCICE 2 ---
// Le connecteur JDBC pour envoyer les données vers le Data Warehouse Postgres [cite: 43]
libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"