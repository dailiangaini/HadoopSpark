package package04.scalaTest3

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 14:00
 */
trait Equal {
  def isEqual(x: Any): Boolean
  def isNotEqual(x:Any):Boolean = !isEqual(x)
}
