package tuning

import org.apache.spark.sql.SparkSession

object CachingDataInMemory {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("caching data in memory")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()

    spark.catalog.cacheTable("")

  }
}
