package batch.rdd

import org.apache.spark.sql.SparkSession

object SQLDemo {
  /*
  Spark SQL批处理应用的代码骨架：
  （1）创建SparkSession
  （2）加载数据
  （3）进行数据分析
  （4）写出数据分析结果
  （5）关闭SparkSession
   */
  def main(args: Array[String]): Unit = {
    val jsonFile =
      "/Users/zhuyufeng/IdeaProjects/LearnSpark/src/main/resources/people.json"
    val spark = SparkSession.builder
      .appName("Hello Spark SQL") // 应用名称
      .master("local") // "local" 本地编译器运行 "yarn" Yarn集群中运行
      .config("truncate", "false")
      .getOrCreate()

    spark.read
      .json(jsonFile)
      .createOrReplaceTempView("people")

    spark.sql("select * from people where age > 20").show()

    spark.close() // 效果等同于 spark.stop()
  }
}
