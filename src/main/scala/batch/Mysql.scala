package batch

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object Mysql {
  def main(args: Array[String]): Unit = {
    val url =
      "jdbc:mysql://XXX:3306/dev_ambari_his"
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
    dataDF.createOrReplaceTempView("mysql_table")
    val source = spark
      .sql("select * from mysql_table")

    val connectionProperties = new Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")

    source.write
      .mode(SaveMode.Append)
      .jdbc(url, "yfzhu_test_2", connectionProperties)

    source.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "dev_ambari_his.yfzhu_test_2")
      .option("user", "")
      .option("password", "")
      .save()

    spark.close()
  }
}
