package package04.scalaTest3

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 13:52
 */
class Employee extends Person {
  var salary = 0.0
  override def toString = super.toString + s"[salary=$salary]"
}
