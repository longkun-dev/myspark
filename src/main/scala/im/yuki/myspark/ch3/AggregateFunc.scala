package im.yuki.myspark.ch3

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/11/16 11:12 PM
 * @version V1.1.0
 * @description aggregate() 行数初探
 */
object AggregateFunc {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("AggregateFunc")
        val sc = new SparkContext(conf)

        sc.setLogLevel("WARN")

        val list = List(1, 2, 3, 4)
        val (mul, sum, count) = sc.parallelize(list, 2)
          .aggregate(zeroValue = (1, 0, 0))((acc, number) =>
              (acc._1 * number, acc._2 + number, acc._3 + 1),
              (x, y) => (x._1 * y._1, x._2 + y._2, x._3 + y._3)
          )
        printf("mul: %d,  count: %d,  sum: %d\n", mul, sum, count)

        val input1 = sc.parallelize(List(1, 2, 5, 6, 10), 2)
        val res0 = input1.aggregate(zeroValue = 10)(addInt, multiplyInt)
        println("res0: " + res0)

        input1.persist(StorageLevel.DISK_ONLY_2)
        input1.unpersist()

        val res1 = input1.top(2)
        res1.foreach(println)

        val res2 = input1.countByValue()
        println("res2: " + res2)

        // 均值
        val res3 = input1.mean()
        println("res3: " + res3)

        val res4 = input1.variance()
        println("res4: " + res4)
    }

    def addInt(num1: Int, num2: Int): Int = {
        num1 + num2
    }

    def multiplyInt(num1: Int, num2: Int): Int = {
        num1 * num2
    }
}
