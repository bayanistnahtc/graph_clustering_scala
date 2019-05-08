import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.io.Source
import scala.util.control.Breaks._

object Main {
  def main(args: Array[String]): Unit = {

    //  val sparkConf = new SparkConf().setAppName("GraphClustering").setMaster("local[*]")
    //  val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .appName("bigass3")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext
    val edges_file = sc.textFile("data/wiki-topcats-short2.txt")
    val vertex_file = sc.textFile("data/wiki-topcats-page-names.txt")
    //    val vertex_file = sc.textFile("data/wiki-topcats-page-names.txt")


    val edgesRDD: RDD[Edge[Long]] = edges_file.map(line => line.split(" "))
      .map(line => Edge(line(0).toLong, line(1).toLong, 2L))

    //First - VertexId, Second - label
    val vertexRDD: RDD[(VertexId, Long)] = vertex_file.map(line => line.split(" "))
      .map(line => (line(0).toLong, line(0).toLong))

    var myGraph = Graph(vertexRDD, edgesRDD)


//    //TakeNeighbourLables
//    def SendMSG(edge: EdgeTriplet[Long, Long]): Iterator[(VertexId, Map[Long, Long])] = {
//      Iterator((edge.dstId, Map(edge.srcAttr -> 1L))) //dstAttr or srcAttr??????????????????????????????????????????
//    }

    def SendMSG(edge: EdgeTriplet[Long, Long]): Iterator[(VertexId, Map[Long, Long])] = {
      Iterator((edge.dstId, Map(edge.srcAttr -> 1L)), (edge.srcId, Map(edge.dstAttr -> 1L)))
    }

    //Count labels
    def MergeMSG(msg1: Map[Long, Long], msg2: Map[Long, Long]): Map[Long, Long] = {
      val msg = msg1.toList ++ msg2.toList
      msg.groupBy(_._1).collect { case e => e._1 -> e._2.map(_._2).sum }
    }

    //MostFrequentLabel
    def VProg(vId: VertexId, label: Long, msgs: Map[Long, Long]): Long = {
      //if there is no message, leave the same label
      var finalLabel = label
      //if there is a message, then return label that occurs most
      if (!msgs.isEmpty) {
        finalLabel = msgs.maxBy(_._2) _1
      }
      finalLabel
    }


    var numOfClusters = Array[Int]()
    var i = 0



    breakable {
      while (i < 20) {
        i += 1
        println("Loop iteration: " + i)
        val g: Graph[VertexId, VertexId] = Pregel(
          graph = myGraph,
          initialMsg = Map.empty[Long, Long], //first - label, second - 1
          maxIterations = 5,
          activeDirection = EdgeDirection.Either //Кому рассылать уже понятно
        )(
          sendMsg = SendMSG,
          mergeMsg = MergeMSG,
          vprog = VProg
        )
        myGraph = g

        numOfClusters = numOfClusters :+ g.vertices.map(_._2).collect().toSet.size

        //        g.collectNeighbors()
        //        val neigbours: EdgeRDD[VertexId] = g.edges
        //        neigbours.map()
        //        g.edges.map(x => (x.srcId, (x.s)))
        //        var tr: Array[EdgeTriplet[VertexId, VertexId]] = g.triplets.collect()

        if (numOfClusters.length >= 2) {
          if (Math.abs(numOfClusters(i - 1) - numOfClusters(i - 2)) < 3) {
            g.vertices.saveAsTextFile("data/outputClusters_" + i.toString)
            break
          }
        }
      }
    }
    val writer = new PrintWriter(new File("data/num_clusters_res.txt"))

    writer.write(numOfClusters.mkString(","))
    writer.close()

    myGraph.vertices.saveAsTextFile("data/output_res")


    //    g.vertices.foreach(println)
    //    println(g.vertices.collectAsMap().keys.toSet.size)


  }
}
