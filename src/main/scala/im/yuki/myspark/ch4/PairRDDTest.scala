package im.yuki.myspark.ch4

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/11/18 12:06 AM
 * @version V1.1.0
 * @description 键值对 RDD，pair RDD
 */
object PairRDDTest {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("JoinTest")
        val sc = new SparkContext(conf)

        sc.setLogLevel("WARN")

        val input1 = sc.parallelize(List("hello world"))

        val map1 = input1.map(item => (item.split(" ")(0), item))
        map1.foreach(item => println("_1: " + item._1 + ",  _2: " + item._2))

        val input2 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
        val res2 = input2.reduceByKey((x, y) => x + y)
        res2.foreach(println)

        val res3 = input2.groupByKey()
        res3.foreach(println)

        val res4 = input2.mapValues(x => x + 5)
        res4.foreach(println)

        val res5 = input2.sortByKey()
        res5.foreach(println)

        println("=====res6")
        val input3 = sc.parallelize(List((3, 9)))
        val res6 = input2.leftOuterJoin(input3)
        res6.foreach(println)

        println("=====res7")
        val res7 = input2.filter({ case (_, value) => value > 2 })
        res7.foreach(println)

        sc.stop()
    }
}
