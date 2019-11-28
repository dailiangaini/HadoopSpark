package package02.scalaTest

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/27 19:02
 */
class Test_3 {
  def addInt(a: Int, b: Int): Int = {
    var sum: Int = 0
    sum = a + b
    return sum
  }

  def printMe(obj: AnyVal): Unit = {
    println(obj)
  }
}
