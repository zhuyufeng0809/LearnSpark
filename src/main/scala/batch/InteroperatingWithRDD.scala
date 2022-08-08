package batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object InteroperatingWithRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("interactive with rdd")
      .master("local")
      .config("truncate", "false")
      .getOrCreate()

    import spark.implicits._

    Seq(Person("Tom", 25)).toDF()
    Seq(Person("Tom", 25)).toDS()
    Seq(1, 2, 3).toDF()
    Seq(1, 2, 3).toDS()
  }
}

case class Person(name: String, age: Int)
