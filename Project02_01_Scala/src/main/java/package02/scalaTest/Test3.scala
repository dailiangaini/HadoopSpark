package package02.scalaTest

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 17:24
 */
object Test3 {

  def addInt(a: Int, b: Int): Int = {
    var sum: Int = 0
    sum = a + b
    return sum
  }

  def printMe(obj: AnyVal): Unit = {
      println(obj)
  }


  def main(args: Array[String]): Unit = {
    printMe(addInt(3, 5))
  }

}
