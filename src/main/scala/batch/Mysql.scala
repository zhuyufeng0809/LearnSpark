package batch

import org.apache.spark.sql.SparkSession

import java.util.Properties

object Mysql {
  def main(args: Array[String]): Unit = {
    val url =
      "jdbc:mysql://:3306/airflow"
    val spark = SparkSession
      .builder()
      .appName("spark mysql")
      .master("local")
      .getOrCreate()
    val dataDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "dev_ambari_his.yfzhu_test_1")
      .option("user", "")
      .option("password", "")
      .option("pushDownLimit", "true")
      .load()
    dataDF.createOrReplaceTempView("mysql_table_1")
    spark
      .sql("select * from mysql_table where task_key >= 1 limit 20")
      .show()

    spark.close()
  }
}
