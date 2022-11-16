package im.yuki.myspark.ch3

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/11/15 11:37 PM
 * @version V1.1.0
 * @description 常见的 RDD 转化操作
 */
object Transform1 {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("Transform1")
        val sc = new SparkContext(conf)

        sc.setLogLevel("WARN")

        val input0 = sc.parallelize(List(1, 4, 9, 10))
        val res0 = input0.reduce((x, y) => x + y)
        println(res0)

        val res1 = input0.reduce((x, y) => x + 1)
        println(res1)

        val input1 = sc.textFile("files/ch3/avg/score*.txt")
        val groupData = input1.map(line => (line.split(" ")(0), line.split(" ")(1).toInt))
          .groupByKey()
          .map(item => {
              var num = 0
              var sum = 0
              for (i <- item._2) {
                  num += 1
                  sum += i
              }
              println(item._1 + ":   avg: " + (sum / num * 1.00F))
          })
        groupData.foreach(println)
    }
}
