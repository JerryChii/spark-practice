package com.chii.spark.graphx.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by jialin5 on 2016/11/7.
  */
object CommonFollow {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    //边的数据类型ED:Int
    val edgeArray = Array(
      (6L, 7L),
      (6L, 1L),
      (6L, 3L),
      (6L, 5L),
      (6L, 9L),

      (1L, 4L),

      (3L, 2L),
      (3L, 4L),

      (5L, 2L),

      (9L, 8L),
      (9L, 1L),
      (9L, 3L),
      (9L, 5L),
      (9L, 6L)
    )

    //构造vertexRDD和edgeRDD
    //val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[(Long, Long)] = sc.parallelize(edgeArray)

    val partitionNum = 4
//    val partitionNum = 900

    val edgesRepartitionRdd = edgeRDD.map {
      case (src, dst) => {
        val pid = PartitionStrategy.EdgePartition2D
          .getPartition(src, dst, partitionNum)
        (pid, (src, dst))
      }
    }
      .partitionBy(new HashPartitioner(partitionNum))
      .map {
        case (pid, (src, dst)) => Edge(src, dst, 1)
      }

    val graph = Graph.fromEdges(edgesRepartitionRdd, 0, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)

    val withAllFollowVertices: VertexRDD[Set[Long]] = graph.aggregateMessages[Set[Long]](
      triplet => {
        triplet.sendToSrc(Set[Long](triplet.dstId))
      },
      (a, b) => a | b
    )

    //排不排序？，暂时先不考虑性能
    //withAllFollowVertices = withAllFollowVertices.mapValues(_.sorted)

    val withAllFollowGraph: Graph[Set[Long], Int] = graph.outerJoinVertices(withAllFollowVertices){
      case (id, name, follow) => follow.getOrElse(Set.empty[Long])
    }

    println("###############################将每个用户的所有关注的对象获取放在用户的点属性上###############################")

    println("**********edges**********")
    withAllFollowGraph.edges.foreach(println(_))
    println("**********vertices**********")
    withAllFollowGraph.vertices.foreach(println(_))

    //求共同关注 -- 两个用户存在关注关系（可以是单向的，比如用户A,B，那么他们必须存在关注关系(A->B, B->A)中的一种或互相关注）
    val sameFollows: Graph[Set[Long], Set[Long]] = withAllFollowGraph.mapTriplets(triplet => triplet.srcAttr & triplet.dstAttr)
    println("###############################将有关注关系的两个用户（可以是单向关注）共同关注的所有对象放在边的属性上###############################")
    println("###############################edges###############################")
    sameFollows.edges.foreach(println(_))
    println("#############################vertices#############################")
    sameFollows.vertices.foreach(println(_))

    //求共同关注 -- 两个用户是互相关注的，求这两个用户的共同关注
    //相互关注的用户存在两条边，只需要针对一条边求值就行了

    /**
      * x.srcAttr.contains(x.dstId) && x.dstAttr.contains(x.srcId)为了找出有共同关注的用户
      * x.srcId < x.dstId是只保留具有共同关注用户的两个好友的一个方向，避免之后的重复计算
      * vpred不能过滤出仅存在epred中的点，就过滤掉空的，减少资源开销
      */
    val mutualFollowGraph = withAllFollowGraph.subgraph(
      epred = e => e.srcAttr.contains(e.dstId) && e.dstAttr.contains(e.srcId) && e.srcId < e.dstId,
      vpred = (id, followed) => followed.nonEmpty
    )

    mutualFollowGraph.unpersist()

    val mutualFollowSameFollows = mutualFollowGraph.mapTriplets(triplet => triplet.srcAttr & triplet.dstAttr)

    println("###############################将相互关注关系的两个用户,edge属性值就是共同关注的好友###############################")
    println("###############################edges###############################")
    mutualFollowSameFollows.edges.foreach(println(_))
    println("#############################vertices#############################")
    mutualFollowSameFollows.vertices.foreach(println(_))

  }



}
