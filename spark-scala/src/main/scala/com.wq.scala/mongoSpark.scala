package com.wq.scala

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bson.Document

object mongoSpark {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("mySpark")
//    //setMaster("local") 本机的spark就用local，远端的就写ip
//    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
//    conf.setMaster("local")
//    val sc = new SparkContext(conf)
//    val readConfig = ReadConfig(Map("spark.mongodb.input.uri" -> "mongodb://localhost:27017/movie_db.movie_ratings", "spark.mongodb.input.readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
//    val writeConfig = WriteConfig(Map("spark.mongodb.input.uri" -> "mongodb://127.0.0.1/movie_db.movie_ratings"))
//
//    // Load the movie rating data from Mongo DB
//    val movieRatings = MongoSpark.load(sc, readConfig).toDF()
//    movieRatings.show(100)
//
//    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
//    MongoSpark.save(documents)

    println("-------------------------------")
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
      .getOrCreate()
    // .config("spark.mongodb.input.readPreference.name", "secondaryPreferred")
    // Reading Mongodb collection into a dataframe
    //读取数据
    val dfRDD = MongoSpark.load(spark)
    println("show:"+dfRDD.show())
    println("show(0):"+dfRDD.show(1))
    println("count:"+dfRDD.count())
    println("first:"+dfRDD.first())
    //sparkSession 是一个封装，产生sc，写入数据到数据库
    val documents = spark.sparkContext.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    MongoSpark.save(documents)
    println("Reading documents from Mongo : OK")
    println("-------------------------------")
  }
}
