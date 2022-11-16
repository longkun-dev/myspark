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
        sc.setLogLevel("WARN")

        val input = sc.parallelize(List(1, 2, 3, 4))
        val res = input.map(x => x * x)
        println(res.collect().mkString(", "))

        val input1 = sc.parallelize(List("Hello world", "Hi", "Hi"))
        // 不会改变 input1 中的数据
        val words = input1.distinct().flatMap(line => line.split(" "))
        words.foreach(println)

        println("处理完毕之后")
        input1.foreach(println)

        val input2 = sc.parallelize(List("Hi", "hi"))
        // 保留重复元素
        val unionRes2 = input1.union(input2)
        println("union: ")
        unionRes2.foreach(println)

        // 会移除重复元素
        val unionRes3 = input1.intersection(input2)
        println("intersection: ")
        unionRes3.foreach(println)

        val unionRes4 = input1.subtract(input2)
        println("subtract: ")
        unionRes4.foreach(println)

        // 数据量大时开销巨大
        val res5 = input1.cartesian(input2)
        println("笛卡尔积: ")
        res5.foreach(println)

        println("取样: ")
        val input5 = sc.parallelize(List(1, 3, 5, 7, 9))
        val sample = input5.sample(withReplacement = true, 0.5, 17)
        sample.foreach(println)

        sc.stop()
    }
}
