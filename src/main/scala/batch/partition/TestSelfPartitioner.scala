package batch.partition

import org.apache.spark.{SparkConf, SparkContext}

object TestSelfPartitioner {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("test-selfPartitioner").setMaster("local")
    val sc = new SparkContext(conf)
    var list = List(
      "beijing1",
      "beijing2",
      "beijing3",
      "tianjin1",
      "tianjin2",
      "tianjin3"
    )

  }
}
