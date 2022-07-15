package batch.partition

import org.apache.spark.Partitioner

class SelfPartitioner(numPart: Int) extends Partitioner {
  override def numPartitions: Int = numPart

  override def getPartition(key: Any): Int = key.toString.hashCode % numPart
}
