ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex02_data_ingestion"
  )

// Dépendances Spark avec le mot-clé "provided" comme demandé [cite: 17, 23]
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"

// Dépendances pour la connexion à MinIO (S3)
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"