package package04.scalaTest3

import java.io.{FileNotFoundException, FileReader, IOException}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 13:57
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    try {
      val f = new FileReader("")
    } catch {
      case ex:FileNotFoundException =>{
        println("FileNotFoundException")
      }
      case ex:IOException =>{
        println("IOException")
      }
    } finally {
      println("finally")
    }
  }
}
