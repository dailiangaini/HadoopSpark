package package01.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/21 16:04
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/Project01_01_Hadoop/input/11")
    val lines = input.flatMap(line => line.split(" "))
    val count = lines.map(word => (word, 1)) .reduceByKey { case (x, y) => x + y }

    //reduceByKey(_+_)是reduceByKey((x,y) => x+y)

    count.foreach(println)
    count.foreach(temp => println(temp._1 + " ~~" + temp._2))
    count.foreach(key => println(key))
    sc.stop()
  }
}
