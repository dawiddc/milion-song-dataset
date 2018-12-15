package com.dawiddc.msd

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

object MillionSongDatasetApp {

  case class Entry(userId: String, trackId: String, listenDate: Int)

  case class Track(versionId: String, trackId: String, artist: String, title: String)

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
      .map(x => Entry(x(0), x(1), x(2).trim.toInt)).toDF()

    val trackInfoDF = spark.sparkContext.textFile("G:/Studia/Semestr 9/EDWD/unique_tracks/unique_tracks.txt")
      .map(_.split("<SEP>"))
      .map(x => Try(Track(x(0), x(1), x(2), x(3))))
      .filter(_.isSuccess).map(_.get).toDF()

    println("1. Top 10 songs:")
    val topTracks = songLogDF.groupBy("trackId").count().sort($"count".desc)
      .limit(10).join(trackInfoDF, usingColumns = Seq("trackId"))
    topTracks.select("title", "artist", "count").sort($"count".desc).show()

    println("2. Top 10 users by distinct songs played:")
    songLogDF.distinct().groupBy("userId").count().sort($"count".desc).show(10)

    println("3. Top 10 most popular artists:")
    val joinedDF = songLogDF.join(trackInfoDF, usingColumns = Seq("trackId"))
    joinedDF.groupBy("artist").count().sort($"count".desc).show(1)

    println("4. Songs listened by month:")
    val dateFormat = new SimpleDateFormat("MM")

    def format: Int => Int = { ts => dateFormat.format(ts * 1000L).toInt }
    import org.apache.spark.sql.functions.udf
    val parseTimestampToMonth = udf(format)
    val songsWithMonthDF = songLogDF.withColumn("listenMonth", parseTimestampToMonth(songLogDF("listenDate")))
    songsWithMonthDF.groupBy("listenMonth").count().sort($"listenMonth").show(12)

    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))
  }

}