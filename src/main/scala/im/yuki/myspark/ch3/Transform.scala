package im.yuki.myspark.ch3

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/11/15 10:59 PM
 * @version V1.1.0
 * @description 常见的转化操作 map & filter
 */
object Transform {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("Transform")
        val sc = new SparkContext(conf)

        val input = sc.parallelize(List(1, 2, 3, 4))
        val res = input.map(x => x * x)
        println(res.collect().mkString(", "))

        val input1 = sc.parallelize(List("Hello world", "Hi", "Hi"))
        // 不会改变 input1 中的数据
        val words = input1.distinct().flatMap(line => line.split(" "))
        words.foreach(println)

        println("处理完毕之后")
        input1.foreach(println)

        sc.stop()
    }
}
