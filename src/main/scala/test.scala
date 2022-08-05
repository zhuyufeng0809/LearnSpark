import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("fuck reimburse").setMaster("local")

    println(
      new SparkContext(conf)
        .textFile("/Users/zhuyufeng/Downloads/amount.txt")
        .map(
          _.replaceAll(".+-", "")
            .replaceAll("\\.\\w+", "")
            .toInt
        )
        .reduce(_ + _)
    )
  }
}
