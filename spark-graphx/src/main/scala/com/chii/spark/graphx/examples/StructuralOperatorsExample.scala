package com.chii.spark.graphx.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jialin5 on 2016/11/3.
  */
object StructuralOperatorsExample {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof")),
      (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi"),
      Edge(4L, 0L, "student"),
      Edge(5L, 0L, "colleague")))
    println("----------------------------subgraph-------------------------------")
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    println("====================before eliminate====================")
    println("=========relation=========")
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    println("=========vertices=========")
    graph.vertices.collect.foreach(println(_))
    println("=========edge=========")
    graph.edges.collect.foreach(println(_))

    println("====================After eliminate(Valid)====================")
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    println("=========vertices=========")
    validGraph.vertices.collect.foreach(println(_))
    println("=========edge=========")
    validGraph.edges.collect.foreach(println(_))
    println("=========relation=========")
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))


    println("----------------------------mask-------------------------------")
    println("vertices和edge必须和输入graph的相同")
    val ccGraph = graph.connectedComponents()
    println("====================Graph connectedComponents====================")
    println("=========edges=========")
    ccGraph.edges.foreach(println(_))
    println("=========vertices=========")
    ccGraph.vertices.foreach(println(_))


    val validCCGraph = ccGraph.mask(validGraph)
    println("====================mask====================")
    println("=========edges=========")
    validCCGraph.edges.foreach(println(_))
    println("=========vertices=========")
    validCCGraph.vertices.foreach(println(_))



  }
}
