import org.apache.spark.sql.SparkSession

object BatchDemo {
  /*
  Spark批处理应用的代码骨架：
  （1）创建SparkSession
  （2）加载数据
  （3）进行数据分析
  （4）写出数据分析结果
  （5）关闭SparkSession
   */
  def main(args: Array[String]): Unit = {
    val logFile = "/opt/spark/spark-3.3.0-hadoop3/README.md"
    val spark = SparkSession.builder
      .appName("Hello Spark Batch") // 应用名称
      .master("local") // "local" 本地编译器运行 "yarn" Yarn集群中运行
      .getOrCreate()
    val logData = spark.read
      .textFile(logFile)
      .cache()
    val numAs = logData
      .filter(line => line.contains("a"))
      .count()
    val numBs = logData
      .filter(line => line.contains("b"))
      .count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.close() // 效果等同于 spark.stop()
  }
}
