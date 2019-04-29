import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GraphClustering").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    var m1 = Map(1 ->2, 3->1)
    var m2 = Map(3 -> 4, 1->5)


    val m5 = m1.toList ++ m2.toList

    val data = m5.groupBy(_._1).collect{
      case e => e._1 -> e._2.map(_._2).sum
    }

    println(data)


//    val reducedByKey = m5.reduceByKey(_ + _).collectAsMap().toMap
//    println(reducedByKey.maxBy(_._2)_1)

  }
}
