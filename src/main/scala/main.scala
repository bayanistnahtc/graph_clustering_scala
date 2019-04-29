import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object main {
  def main(args: Array[String]): Unit = {

  val sparkConf = new SparkConf().setAppName("GraphClustering").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val edges_file = sc.textFile("data/wiki_small.txt")
  val vertex_file = sc.textFile("data/wiki_names_small.txt")


  val edgesRDD: RDD[Edge[Long]] = edges_file.map(line => line.split(" "))
    .map(line => (Edge(line(0).toLong, line(1).toLong, 2L)))

    //First - VertexId, Second - label
  val vertexRDD: RDD[(VertexId, Long)] = vertex_file.map(line => line.split(" "))
      .map(line => (line(0).toLong, line(0).toLong ))

  val myGraph = Graph(vertexRDD, edgesRDD)



  //TakeNeighbourLables
    def SendMSG(edge: EdgeTriplet[Long, Long]): Iterator[(VertexId, Map[Long, Long])] = {
        Iterator((edge.dstId, Map(edge.srcAttr -> 1L)))//dstAttr or srcAttr??????????????????????????????????????????
    }
  //Count labels
    def MergeMSG(msg1:Map[Long, Long], msg2:Map[Long, Long]):Map[Long, Long]={
      val msg = msg1.toList ++ msg2.toList
//      val reduced = msg.reduceByKey(_ + _)//didnt work with pregel
//      reduced.collectAsMap().toMap
      msg.groupBy(_._1).collect{case e => e._1 -> e._2.map(_._2).sum }
    }
  //MostFrequentLabel
    def VProg(vId: VertexId, label: Long, msgs:Map[Long, Long] ): Long={
      //if there is no message, leave the same label
      var finalLabel = label
      //if there is a message, then return label that occurs most
      if (!msgs.isEmpty){
        finalLabel = msgs.maxBy(_._2)_1
      }
      finalLabel
    }


    val g = Pregel(
      graph = myGraph,
      initialMsg = Map.empty[Long, Long], //first - label, second - 1
      maxIterations = 10,
      activeDirection = EdgeDirection.Either//Кому рассылать непонятно
    )(
      sendMsg = SendMSG,
      mergeMsg = MergeMSG,
      vprog = VProg
    )

    g.vertices.foreach(println)

  }
}
