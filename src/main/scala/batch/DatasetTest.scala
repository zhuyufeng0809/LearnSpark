package batch

import org.apache.spark.sql.SparkSession

object DatasetTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Hello Spark Dataset")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()
    val ds_json = spark.read.json(
      "/Users/zhuyufeng/IdeaProjects/LearnSpark/src/main/resources/people.json"
    )
    ds_json.printSchema()
    ds_json.show()
  }
}
