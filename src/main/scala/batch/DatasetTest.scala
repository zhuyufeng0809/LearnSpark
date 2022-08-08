package batch

import org.apache.spark.sql.SparkSession

object DatasetTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Hello Spark Dataset")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()
    import spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    println(primitiveDS.map(_ + 1).collect().mkString("Array(", ", ", ")"))
  }
}
