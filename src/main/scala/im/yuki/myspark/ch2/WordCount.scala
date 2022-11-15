package im.yuki.myspark.ch2

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/9/4 4:11 PM
 * @version V1.1.0
 * @description spark 进行单词数量统计
 */
object WordCount {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("WordCount")
        val sc = new SparkContext(conf)

        val input = sc.textFile(args(0))
        val words = input.flatMap(line => line.split(" "))
        val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
        counts.saveAsTextFile(args(1))

        sc.stop()
    }
}
