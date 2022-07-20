package batch

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("Broadcast Demo").setMaster("local")
    val sc = new SparkContext(conf)
    val broadcastVar = sc.broadcast(Array[Int](1, 2, 3))
    val list =
      List("beijing", "beijing", "beijing", "shanghai", "shanghai", "tianjin")
    val rdd = sc.parallelize(list)
    rdd.foreach(_ => println(broadcastVar.value.mkString("") + "" + _))

    broadcastVar.unpersist()
    broadcastVar.destroy()

    sc.stop()
  }
}
