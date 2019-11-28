package package04.scalaTest3

import java.io.{File, PrintWriter}

import scala.io.{Source, StdIn}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 14:29
 */
object Test5 {

  def test1: Unit ={
    val writer = new PrintWriter(new File("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/Project01_01_Hadoop/input/22"))

    writer.write("123\n1234")
    writer.close()
  }

  def test2:Unit = {
    println("请输入")
    val line = StdIn.readLine()
    println(line)
  }

  def test3:Unit = {
    val source = Source.fromFile(new File("/Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/Project01_01_Hadoop/input/22"))
    println("_______")
    println()
    println("_______")
    source.getLines().toArray.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    test3
  }
}
