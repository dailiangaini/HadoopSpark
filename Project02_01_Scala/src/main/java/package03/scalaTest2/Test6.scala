package package03.scalaTest2

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 13:00
 */
object Test6 {
  def main(args: Array[String]): Unit = {
    val myMap: Map[String, String] = Map("key" -> "value")
    val maybeString: Option[String] = myMap.get("key")
    println(maybeString)
    if(!maybeString.isEmpty){
      println(maybeString.get)
    }

    val maybeString2: Option[String] = myMap.get("key2")
    println(maybeString2)
  }
}
