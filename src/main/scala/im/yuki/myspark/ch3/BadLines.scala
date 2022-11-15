package im.yuki.myspark.ch3

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author longkun
 * @date 2022/11/14 11:38 PM
 * @version V1.1.0
 * @description 找出警告和错误级别的日志消息
 *              spark-submit --class im.yuki.myspark.ch3.BadLines \
 *              target/scala-2.12/myspark_2.12-1.1.0.jar > \
 *              files/ch3/bad_lines/result.txt
 */
object BadLines {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local")
          .setAppName("BadLines")
        val sc = new SparkContext(conf)

        val log1 = sc.textFile("files/ch3/bad_lines/1.log")
        val log2 = sc.textFile("files/ch3/bad_lines/2.log")

        val warnings1 = log1.filter(line => line.contains("WARNING"))
        val warnings2 = log2.filter(line => line.contains("WARNING"))
        val errors1 = log1.filter(line => line.contains("ERROR"))
        val errors2 = log2.filter(line => line.contains("ERROR"))

        val badLines = warnings1.union(warnings2).union(errors1).union(errors2)
        badLines.foreach(e => println(e))

        badLines.take(2).foreach(println)

        println("处理完成")
        sc.stop()
    }
}
