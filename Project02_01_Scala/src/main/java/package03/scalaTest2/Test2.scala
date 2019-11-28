package package03.scalaTest2

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 11:20
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    var site: List[String] = List("Google", "Baidu", "aLi")

    var nums: List[Int] = List(1, 2,3, 4)

    val empty: List[Nothing] = List()

    var dim: List[List[Int]] = List(
      List(1,2),
      List(3,4)
    )


    println(dim.head)
    println(dim.tail)

    /**
     * (1) :: 该方法被称为cons，意为构造，向队列的头部追加数据，创造新的列表。用法为 x::list,其中x为加入到头部的元素，无论x是列表与否，
     *    它都只将成为新生成列表的第一个元素，也就是说新生成的列表长度为list的长度＋1(btw, x::list等价于list.::(x))
     *
     * (2) :+和+: 两者的区别在于:+方法用于在尾部追加元素，+:方法用于在头部追加元素，和::很类似，但是::可以用于pattern match ，而+:则不行.
     *    关于+:和:+,只要记住冒号永远靠近集合类型就OK了。
     *
     * (3) ++ 该方法用于连接两个集合，list1++list2
     *
     * (4) ::: 该方法只能用于连接两个List类型的集合
     */

    var site2 = "123" :: "1234" :: Nil
    println(site2)

    val site3 = "北京小辉的博客" :: ("Google" :: ("Baidu" :: Nil))
    println(site3)

  }
}
