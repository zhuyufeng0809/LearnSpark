package batch

import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("Spark Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    val selfAccumulator = new SelfAccumulator
    sc.register(selfAccumulator, "my fucking Accumulator")
    val list =
      List("beijing", "shanghai", "shanghai", "tianjin")
    sc.parallelize(list).foreach(selfAccumulator.add)

    println(selfAccumulator.value)
  }
}
