package batch

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SpecifySchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("specify schema")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()

    // 1.从已有RDD创建Row类型RDD
    val peopleRDD =
      spark.sparkContext.textFile("examples/src/main/resources/people.txt")
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // 2.创建`StructType`来表示Schema
    val schemaString = "name age"
    val fields = schemaString
      .split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // 3.通过`SparkSession`实例的`createDataFrame`方法将`StructType`应用于RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")
    spark.sql("SELECT name FROM people").show()
  }
}
