package batch

import org.apache.spark.sql.SparkSession

object SqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark SQL Test")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()
    val df_json = spark.read.json(
      "/Users/zhuyufeng/IdeaProjects/LearnSpark/src/main/resources/people.json"
    )
    df_json.createOrReplaceTempView("people")
    import spark.implicits._
    val df = Seq(("Alex", "shanghai"), ("Andy", "beijing")).toDF("name", "city")
    df.createOrReplaceTempView("location")
    spark
      .sql(
        "select a.*, b.city from people a inner join location b on a.name = b.name"
      )
      .show()
  }
}
