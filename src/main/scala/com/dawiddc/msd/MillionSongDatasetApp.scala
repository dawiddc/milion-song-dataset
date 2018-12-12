package com.dawiddc.msd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MillionSongDatasetApp {

  case class Entry(userId: String, songId: String, listenDate: Int)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Million Song Dataset Sql")
      .config(new SparkConf().setAppName("Million Song Dataset App").setMaster("local[*]"))
      .getOrCreate()
    import spark.implicits._

    val start = System.currentTimeMillis
    val songLogDF = spark.sparkContext.textFile("G:/Studia/Semestr 9/EDWD/triplets_sample_20p/triplets_sample_20p.txt")
      .map(_.split("<SEP>"))
      .map(x => Entry(x(0), x(1), x(2).trim.toInt))
      .toDF()

    songLogDF.show()

    println("Top 10 songs:")
    //    songLogDF.groupBy("songId").count().sort($"count".desc).show(10)
    println("Top 10 users by distinct songs played:")
    songLogDF.distinct().groupBy("userId").count().sort($"count".desc).show(10)

    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))
  }
}