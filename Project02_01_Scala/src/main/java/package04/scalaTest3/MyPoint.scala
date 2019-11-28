package package04.scalaTest3

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 14:23
 */
class MyPoint(xc:Int, yc: Int) extends Equal {
  var x: Int = xc
  var y: Int = yc
  override def isEqual(obj: Any): Boolean = {
    obj.isInstanceOf[MyPoint] && obj.asInstanceOf[MyPoint].x == x && obj.asInstanceOf[MyPoint].y == y
  }
}
