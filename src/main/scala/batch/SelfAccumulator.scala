package batch

import org.apache.spark.util.AccumulatorV2

class SelfAccumulator extends AccumulatorV2[String, String] {

  private var internal: String = "internal"

  override def isZero: Boolean = internal.equals("internal")

  override def copy(): AccumulatorV2[String, String] = {
    val copy = new SelfAccumulator
    copy.internal = internal
    copy
  }

  override def reset(): Unit = internal = "internal"

  override def add(v: String): Unit = internal += v

  override def merge(other: AccumulatorV2[String, String]): Unit =
    internal = other.value

  override def value: String = internal
}
