import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    SparkSession.builder().appName("hello world")
  }
}
