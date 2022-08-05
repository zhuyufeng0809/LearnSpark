package batch

import org.apache.spark.{SparkConf, SparkContext}

object Closure {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("Spark Closure").setMaster("local")
    val sc = new SparkContext(conf)
    val list =
      List("beijing", "shanghai", "shanghai", "tianjin")
    val rdd = sc.parallelize(list)

    rdd.foreach(println)
    rdd.map(println).unpersist(blocking = true)
  }
}
