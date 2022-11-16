package im.yuki.myspark.ch3

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/11/16 10:43 PM
 * @version V1.1.0
 * @description fold() 行数初探
 */
object FoldFunc {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("FoldFunc")
        val sc = new SparkContext(conf)

        sc.setLogLevel("WARN")

        val input0 = sc.parallelize(List(1, 2, 3, 4), 3)
        val res0 = input0.reduce((x, y) => x + y)
        println("res0: " + res0)

        // (1 + 2 + 3 + 4 + 10) + (10 + 10 + 10)
        val res1 = input0.fold(zeroValue = 10)((x, y) => x + y)
        println("res1: " + res1)

        // 二元组规约
        val res2 = input0.map(item => (item, 1)).reduce((key, value) => (key._2 + value._2, key._1 + value._1))
        println("avg: " + res2._1.toFloat / res2._2)
    }

}
