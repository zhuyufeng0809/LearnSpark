package stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

object StructuredDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("Structured Network WordCount")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    words.groupBy(
      window($"", "", ""),
      $""
    )
    val wordCounts = words
      .withWatermark("", "")
      .groupBy("value")
      .count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
