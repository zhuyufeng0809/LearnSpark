package batch

import org.apache.spark._

object TestPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test-partitioner").setMaster("local")
    val sc = new SparkContext(conf)
    val list = List("1", "2", "3", "4", "5", "6")
    //仅键值对RDD才有分区器，非键值对RDD的分区器取值为None
    val rdd = sc.parallelize(list).map(k => (k, "value_" + k))

    rdd.foreach(_ =>
      println("default partition is " + TaskContext.getPartitionId())
    )

    rdd
      .partitionBy(new HashPartitioner(3))
      .foreach(_ =>
        println("hash partition is " + TaskContext.getPartitionId())
      )

    rdd
      .partitionBy(new RangePartitioner(3, rdd, true, 3))
      .foreach(_ =>
        println("range partition is " + TaskContext.getPartitionId())
      )

    sc.stop()
  }
}
