package package03.scalaTest2

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 13:00
 */
object Test7 {
  def matchTest(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => "many"
  }
  def matchTest2(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "scala Int"
    case _ => "many"
  }

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    println(matchTest(3))
    println(matchTest2(3))

    val alice = new Person("Alice", 25)
    val bob = new Person("Bob", 32)
    val charlie = new Person("Charlie", 32)

    for (elem <- List(alice, bob, charlie)) {
      elem match {
        case Person("Alice", 25) => println("Alice")
        case Person("Bob", 32) => println("Bob")
        case Person("Charlie", 32) => println("Charlie")
      }
    }

  }
}
