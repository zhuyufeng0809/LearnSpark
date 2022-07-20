package batch

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object TestSelfPartitioner {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("test-selfPartitioner").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List(
      "beijing1",
      "beijing2",
      "beijing3",
      "tianjin1",
      "tianjin2",
      "tianjin3"
    )

    val rdd = sc.parallelize(list)
    rdd
      .map((_, 1))
      .partitionBy(new SelfPartitioner(3))
      .foreachPartition({
        println("partitionNum:" + TaskContext.getPartitionId())
        _.foreach(println(_))
      })

    sc.stop()
  }
}
