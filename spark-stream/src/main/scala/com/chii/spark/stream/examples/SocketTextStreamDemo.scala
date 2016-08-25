package com.chii.spark.stream.examples

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Describe:   
  * Author:  JerryChii.
  * Date:    2016/8/24
  */
object SocketTextStreamDemo {
  def main(args: Array[String]) {
    val hostName = "localhost"
    val port = 8888

    val conf = new SparkConf().setMaster("local[2]").setAppName("SocketTextStreamDemo")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream(hostName, port, StorageLevel.MEMORY_ONLY_SER)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    result.print()
    util.HashMap
    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }

}
