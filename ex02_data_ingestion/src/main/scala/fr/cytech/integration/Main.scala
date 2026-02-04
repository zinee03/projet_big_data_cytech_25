package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    // 1. Correction Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("Ex02 - Data Cleaning and Ingestion")
      .master("local[*]")
      .getOrCreate()

    // 2. Configuration MinIO
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    sc.hadoopConfiguration.set("fs.s3a.access.key", "minio")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    sc.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    sc.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "array")

    println("Démarrage du pipeline de données...")

    try {
      // 3. Lecture depuis nyc-raw
      val df = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2024-01.parquet")

      // 4. Nettoyage : filtrer les données selon le contrat [cite: 44]
      // Nettoyage plus strict : on enlève les prix/distances <= 0 ET les valeurs nulles
      val cleanDf = df.filter(
        col("total_amount") > 0 &&
          col("trip_distance") > 0 &&
          col("passenger_count").isNotNull &&
          col("PULocationID").isNotNull &&
          col("DOLocationID").isNotNull
      )

      val rowCount = cleanDf.count()
      println(s"Nombre de lignes valides : $rowCount")

      if (rowCount == 0) {
        println("ATTENTION : Aucune donnée ne correspond aux filtres. Le fichier sera vide.")
      } else {
        // --- BRANCHE 1 : Sauvegarde sur MinIO pour le ML [cite: 45, 53] ---
        println("Branche 1 : Envoi vers le bucket nyc-processed...")
        cleanDf.write
          .mode(SaveMode.Overwrite)
          .parquet("s3a://nyc-processed/yellow_tripdata_2024-01_clean.parquet")

        // --- BRANCHE 2 : Ingestion PostgreSQL (Data Warehouse) [cite: 48, 49, 55] ---
        println("Branche 2 : Ingestion vers PostgreSQL...")

        val jdbcUrl = "jdbc:postgresql://localhost:5432/nyc_db"
        val connectionProperties = new Properties()
        connectionProperties.put("user", "postgres")
        connectionProperties.put("password", "postgres")
        connectionProperties.put("driver", "org.postgresql.Driver")

        // La table 'trips' doit être créée via l'exercice 3 au préalable [cite: 49, 65]
        cleanDf.write
          .mode(SaveMode.Append)
          .jdbc(jdbcUrl, "trips", connectionProperties)

        println("Exercice 2 : Ingestion multi-branche terminée avec succès !")
      }

    } catch {
      case e: Exception => println(s"Erreur critique : ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}