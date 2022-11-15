package im.yuki.myspark.ch2

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/11/14 11:04 PM
 * @version V1.1.0
 * @description 初始化 Spark
 */
object InitSpark {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("InitSpark")
        val sc = new SparkContext(conf)

        val data = sc.parallelize(List("ab", "cd"))
        println(data.count())
        println(data.first())

        println("initial successfully")

        sc.stop()
    }
}
