package fr.cytech.integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // 1. Correction Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("Ex02 - Data Cleaning")
      .master("local[*]")
      .getOrCreate()

    // 2. Configuration MinIO (identifiants de ton image)
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    sc.hadoopConfiguration.set("fs.s3a.access.key", "minio")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    sc.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // --- ASTUCE WINDOWS : Évite d'utiliser le disque local pour le cache ---
    sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    sc.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "array")

    println("Nettoyage des données en cours...")

    try {
      // 3. Lecture depuis nyc-raw
      val df = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2024-01.parquet")

      // 4. Nettoyage : filtrer les données aberrantes
      val cleanDf = df.filter(col("total_amount") > 0 && col("trip_distance") > 0)

      // 5. Sauvegarde sur nyc-processed (Vérifie que le bucket existe !)
      println("Envoi vers le bucket nyc-processed...")
      cleanDf.write
        .mode("overwrite")
        .parquet("s3a://nyc-processed/yellow_tripdata_2024-01_clean.parquet")

      println("Exercice 2 : Branche 1 (MinIO) terminée avec succès !")

    } catch {
      case e: Exception => println(s"Erreur : ${e.getMessage}")
    }

    spark.stop()
  }
}