package batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object UDFFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark UDF")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()
    val df_json = spark.read.json(
      "/Users/zhuyufeng/IdeaProjects/LearnSpark/src/main/resources/people.json"
    )
    df_json.createOrReplaceTempView("people")

    val plusOne = udf((x: Int) => x + 1)
    spark.udf.register("plusOne", plusOne)
    spark.udf.register(
      "age_plus",
      (age: Int, step: Int) => {
        age + step
      }
    )
    spark
      .sql(
        "select age, plusOne(age) as next_age, age_plus(age, 12) as next_generation, name from people"
      )
      .show()
  }
}
