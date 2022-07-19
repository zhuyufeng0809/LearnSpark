package batch.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark DataFrame Schema")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()
    val df_text = spark.read.text("your_file_path/kv1.txt")
    val df_csv = spark.read
      .format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("your_file_path/people.csv")
    val df_json = spark.read.json("your_file_path/people.json")
    val df_parquet = spark.read.parquet("your_file_path/users.parquet")
    val df_orc = spark.read.orc("your_file_path/users.orc")

    df_text.write.text("your_file_path/text_result")
    df_csv.write.csv("your_file_path/csv_result")
    df_json.write.json("your_file_path/json_result")
    df_parquet.write.parquet("your_file_path/parquet_result")
    df_orc.write.orc("your_file_path/orc_result")
  }
}
