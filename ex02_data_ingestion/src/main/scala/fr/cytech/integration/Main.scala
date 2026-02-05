package fr.cytech.integration

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {

    // 1. Config Windows (Obligatoire)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("Ex02_Ingestion_Finale")
      .master("local[*]")
      // --- Optimisation Windows ---
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      // --- Config MinIO ---
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    println("--- Démarrage de l'Ingestion ---")

    try {
      // 2. Lecture
      val rawDf = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2024-01.parquet")

      // 3. Nettoyage et Renommage (Adapté à la nouvelle table SQL)
      val cleanDf = rawDf
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") // On le garde cette fois !
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("payment_type", "payment_type_id")
        .filter(col("total_amount") > 0 && col("trip_distance") > 0)
        .select(
          col("vendor_id").cast("int"),
          col("rate_code_id").cast("int"),
          col("payment_type_id").cast("int"),
          col("pickup_location_id").cast("int"),
          col("dropoff_location_id").cast("int"),
          col("pickup_datetime"),
          col("dropoff_datetime"), // Incluse dans la nouvelle table
          col("passenger_count").cast("int"),
          col("trip_distance"),
          col("fare_amount"),
          col("extra"),
          col("mta_tax"),
          col("tip_amount"),
          col("tolls_amount"),
          col("improvement_surcharge"),
          col("total_amount"),
          col("congestion_surcharge")
        )

      val count = cleanDf.count()
      println(s"--- Lignes prêtes : $count ---")

      if (count > 0) {
        // Branche 1 : MinIO
        println(">>> Écriture MinIO...")
        cleanDf.write.mode(SaveMode.Overwrite).parquet("s3a://nyc-processed/yellow_tripdata_2024-01_clean.parquet")
        println(" MinIO : OK")

        // Branche 2 : Postgres
        println(">>> Ingestion PostgreSQL...")

        // --- CONFIGURATION CRITIQUE ---
        // On essaie d'abord 'password'. Si ça rate, change ici pour 'toto'.
        val dbPassword = "password"

        val jdbcUrl = "jdbc:postgresql://localhost:5432/taxi_warehouse"
        val props = new Properties()
        props.put("user", "admin")
        props.put("password", dbPassword)
        props.put("driver", "org.postgresql.Driver")

        cleanDf.write
          .mode(SaveMode.Append)
          .jdbc(jdbcUrl, "fact_trips", props) // Table en minuscule

        println(" PostgreSQL : Données insérées avec succès !")
      }

    } catch {
      case e: Exception =>
        println(s" ERREUR : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}