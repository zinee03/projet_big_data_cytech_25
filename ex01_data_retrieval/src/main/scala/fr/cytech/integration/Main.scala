package fr.cytech.integration

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Correction pour Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("Ex01 - NYC Taxi Retrieval")
      .master("local[*]")
      .getOrCreate()

    // Configuration Minio avec tes vrais identifiants
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    sc.hadoopConfiguration.set("fs.s3a.access.key", "minio")    // Modifié selon ton image
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "minio123") // Modifié selon ton image
    sc.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // Astuce pour éviter l'erreur de permission locale sur Windows
    sc.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    sc.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "array")

    println("Transfert vers Minio en cours...")

    // Lecture locale et écriture sur Minio [cite: 38, 39]
    val df = spark.read.parquet("data/raw/yellow_tripdata_2024-01.parquet")
    df.write
      .mode("overwrite")
      .parquet("s3a://nyc-raw/yellow_tripdata_2024-01.parquet")

    println("Succès ! Le fichier est sur Minio.")
    spark.stop()
  }
}