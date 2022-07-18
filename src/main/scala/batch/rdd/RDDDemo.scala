package batch.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDDemo {
  /*
  Spark RDD API应用的代码骨架：
  （1）初始化SparkContext
  （2）加载数据并转换为RDD
  （3）对RDD执行Transformation操作，这里是懒加载。然后在Spark内部构建RDD算子的依赖关系，也就是DAG，但不执行
  （4）对RDD执行Action操作，触发执行后，当Spark遇到Action操作时，就会从任务中划分出一个作业并提交和执行这个作业
  （5）若任务执行完毕，则停止SparkContext，释放资源
   */
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("Spark RDD API Demo").setMaster("local")
    val sc = new SparkContext(conf)
    val list =
      List("beijing", "beijing", "beijing", "shanghai", "shanghai", "tianjin")
    val rdd = sc.parallelize(list)
    rdd.map((_, 1)).reduceByKey(_ + _).collect().foreach(println(_))

    sc.stop()
  }
}
