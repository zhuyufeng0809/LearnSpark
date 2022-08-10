package batch

import org.apache.spark.sql.SparkSession

object Catalog {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("catalog")
      .master("local")
      .getOrCreate()

  }
}
