package com.chii.spark.graphx.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by jialin5 on 2016/11/4.
  */
object VertexAndEdgeAttr {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext


    val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 9L).map(id => (id, 1)))
    println("=============================setA=============================")
    setA.foreach(println(_))
    val rddB: RDD[(VertexId, Double)] = sc.parallelize(0L until 10L).flatMap(id => List((id, 1.0), (id, 2.0)))
    println("=============================rddB=============================")
    rddB.foreach(println(_))
    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)
    println("=============================setB=============================")
    setB.foreach(println(_))

//    val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 100L).map(id => (id, 1)))
//    val rddB: RDD[(VertexId, Double)] = sc.parallelize(0L until 100L).flatMap(id => List((id, 1.0), (id, 2.0)))
//    // There should be 200 entries in rddB
//    rddB.count
//    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)
//    // There should be 100 entries in setB
//    setB.count
//    // Joining A and B should now be fast!
//    val setC: VertexRDD[Double] = setA.innerJoin(setB)((id, a, b) => a + b)
  }
}
