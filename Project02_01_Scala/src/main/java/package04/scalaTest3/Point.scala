package package04.scalaTest3

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 13:48
 */
class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int =yc

  def move(dx: Int, dy: Int)={
    x += dx
    y += dy
    println("x="+x)
    println("y="+y)
  }
}
