package com.chii.spark.graphx.examples

/**
 * Created by manbu on 16/9/23.
 */
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.reflect.ClassTag

/**
 * Created by wangshun on 16/5/25.
 */
object BridgeUserDetector {
  val regex = """\d+""".r

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val sp = '\t': Char
    val edgeRdd = sc.textFile(args(0)).
      filter(regex.findPrefixOf(_) != None).map { line =>
      val fields = line.split(sp)
      //Edge(fields(0).toLong, fields(1).toLong, 1)
      (fields(0).toLong, fields(1).toLong)
    }
    val partitionNum = 900
    val edgesRepartitionRdd = edgeRdd.map {
      case (src, dst)
      => val pid = PartitionStrategy.EdgePartition2D
        .getPartition(src, dst, partitionNum)
        (pid, (src, dst))
    }
      .partitionBy(new HashPartitioner(partitionNum))
      .map {
      case (pid, (src, dst)) =>
        Edge( src, dst, 1)
    }

    //构建图 顶点Int类型
    val g = Graph.fromEdges(edgesRepartitionRdd, 0, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
    //val g = GraphLoader.edgeListFile(sc,"target/classes/resource/follow",true)


    BridgeUserDetector.TwoHopsNeighborAlg.run(g)
      .vertices
      .mapValues {
      _.filter(_._2._1 == 2)
    }
      .mapValues(_.mapValues(_._2))
      .filter(_._2.nonEmpty)
      .saveAsTextFile(args(1))

  }

  type SPMap = Map[VertexId, (Int, Set[VertexId])]

  /**
   * Computes shortest paths to the given set of landmark vertices, returning a graph where each
   * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
   */
  object TwoHopsNeighborAlg {
    /** Stores a map from the vertex id of a landmark to the distance to that landmark. */

    val default = (Int.MaxValue, Set[VertexId]())

    private def makeMap(x: (VertexId, (Int, Set[VertexId]))*) = Map(x: _*)

    private def incrementMap(spmap: SPMap, srcId: VertexId, dstId: VertexId): SPMap
    = spmap.map { case (v, (d, l)) => v ->(d + 1, if (v == dstId) l.+(srcId) else l) }

    private def mergeMsg(spmap: SPMap, msgmap: SPMap): SPMap =
      (spmap.keySet ++ msgmap.keySet).map {

        k => k -> {
          if (spmap.getOrElse(k, default)._1 < msgmap.getOrElse(k, default)._1) {
            spmap.get(k).get
          } else if (spmap.getOrElse(k, default)._1 == msgmap.getOrElse(k, default)._1) {
            Tuple2[Int, Set[VertexId]](msgmap.get(k).get._1, msgmap.get(k).get._2.union(spmap.get(k).get._2))
          } else {
            msgmap.get(k).get
          }
        }
      }.toMap

    /**
     * Computes shortest paths to the given set of landmark vertices.
     *
     * @tparam ED the edge attribute type (not used in the computation)
     *
     * @param graph the graph for which to compute the shortest paths
     *
     * @return a graph where each vertex attribute is a map containing the shortest-path distance to
     *         each reachable landmark vertex.
     */
    def run[VD, ED: ClassTag](graph: Graph[VD, ED]): Graph[SPMap, ED] = {
      val spGraph = graph.mapVertices { (vid, attr) =>
        makeMap(vid ->(0, Set[VertexId]()))
      }

      val initialMessage = makeMap()

      def vprog(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
        mergeMsg(attr, msg)
      }

      def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
        val newAttr = incrementMap(edge.dstAttr, edge.srcId, edge.dstId)
        if (edge.srcAttr != mergeMsg(edge.srcAttr, newAttr))
          Iterator((edge.srcId, newAttr))
        else
          Iterator.empty

      }

      Pregel(spGraph, initialMessage, 2, EdgeDirection.Out)(vprog, sendMessage, mergeMsg)
    }
  }

}
