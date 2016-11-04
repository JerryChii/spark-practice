package com.chii.spark.graphx.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by jialin5 on 2016/11/3.
*/
object AggregationExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
//    val graph: Graph[Double, Int] =
//    GraphGenerators.logNormalGraph(sc, numVertices = 10).mapVertices( (id, _) => id.toDouble )
//    println("====================generate graph====================")
//    println("=========vertices=========")
//    graph.vertices.collect.foreach(println(_))
//    println("=========edge=========")
//    graph.edges.collect.foreach(println(_))

    /** 图关系可参照resources/example_graph.gif */
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    println("=======================计算src的年龄大于dst年龄所有人的年龄平均值=======================")
    // Compute the number of older followers and their total age
    var olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr._2 > triplet.dstAttr._2) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst((1, triplet.srcAttr._2))
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    var avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues( (id, value) =>
      value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
    // $example off$

    /**
      * 使用TripletFields指定Fields可以优化处理，要怎么设定主要看sendMsg方法中需要哪些数据
      */
    println("--------")
    olderFollowers = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr._2 > triplet.dstAttr._2) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst((1, triplet.srcAttr._2))
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2), // Reduce Function
      TripletFields.All
    )
    // Divide total age by number of older followers to get average age of older followers
    avgAgeOfOlderFollowers =
    olderFollowers.mapValues( (id, value) =>
      value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))


    println("=======================计算入度=======================")
    var inDegree: VertexRDD[Int] = graph.aggregateMessages[Int](
      triplet => triplet.sendToDst(1),
      _+_
    )
    inDegree.collect.foreach(println(_))
    println("--------")
    inDegree = graph.aggregateMessages[Int](
      triplet => triplet.sendToDst(1),
      _+_,
      TripletFields.None
    )
    inDegree.collect.foreach(println(_))


    println("=======================计算出度=======================")
    val outDegree: VertexRDD[Int] = graph.aggregateMessages[Int](
      triplet => triplet.sendToSrc(1),
      _+_,
      TripletFields.None
    )
    outDegree.collect.foreach(println(_))


    println("=======================计算度=======================")
    val inOutDegree: VertexRDD[Int] = graph.aggregateMessages[Int](
      triplet => {
        triplet.sendToSrc(1)
        triplet.sendToDst(1)
      },
      _+_,
      TripletFields.None
    )
    inOutDegree.collect.foreach(println(_))

    /**
      * These operators can be quite costly as they duplicate information and require substantial communication.
      * If possible try expressing the same computation using the aggregateMessages operator directly.
      */
    println("=======================collectNeighborIds=======================")
    graph.collectNeighborIds(EdgeDirection.In).foreach{case (id, neighborIds) => {
      print(s"$id: ")
      neighborIds.foreach(nid => print(s"$nid,"))
      println
    }}
    println("=======================collectNeighbors=======================")
    graph.collectNeighbors(EdgeDirection.Either).foreach{case (id, neighbor) => {
      print(s"$id: ")
      neighbor.foreach(nei => print(s"$nei,"))
      println
    }}

    sc.stop()
  }
}
