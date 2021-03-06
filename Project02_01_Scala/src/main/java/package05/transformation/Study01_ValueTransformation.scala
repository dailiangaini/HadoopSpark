package package05.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 15:17
 */
object Study01_ValueTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("StudySpark")
    val sc = new SparkContext(conf)

    /**
     * 1.map
     * 数据集中的每个元素经过用户自定义函数转换形成一个新的RDD
     */
    def testMap():Unit={
      val list = List("dog", "cat", "salmon")
      val listValue: RDD[String] = sc.parallelize(list)
      val value: RDD[Int] = sc.parallelize(list).map(_.length)
      value.foreach(println)
      /**
       * 3
       * 3
       * 6
       */
      val unit: RDD[(String, Int)] = listValue.zip(value)
      unit.foreach(println)

      /**
       * (dog,3)
       * (cat,3)
       * (salmon,6)
       */
    }

    /**
     * 2.flatMap
     * 与map类似，每个元素都可以被映射到0个或者多个输出项，最终结果，扁平化输出
     */
    def testFlatMap(): Unit ={
      val list: List[Int] = (1 to 5).toList
      val listRdd: RDD[Int] = sc.parallelize(list)
      val unit: RDD[Int] = listRdd.flatMap(1 to _)
      val array: Array[Int] = unit.collect()
      array.foreach(item =>{
        print(item + " ")
      })

      /**
       * 1 1 2 1 2 3 1 2 3 4 1 2 3 4 5
       */
    }

    /**
     * 3.mapPartitions
     * 每个分区的执行，调用次数与分区个数相同
     */
    def testMapPartitions(): Unit = {
      val l = List(("kppo", "aa"), ("kppo1", "vv"), ("kppo2", "bb"), ("aini", "aa"))
      val listRdd: RDD[(String, String)] = sc.parallelize(l, 2)
      val value: RDD[(String, String)] = listRdd.mapPartitions(item => item.filter(_._2 != "aa"))
      value.foreachPartition(item => println(item.toList))

      /**
       * List((kppo1,vv))
       * List((kppo2,bb))
       */
    }

    /**
     *  4.glom
     *  将RDD的每个分区中类型为T的元素转换为数组Array[T]
     */
    def testGlop(): Unit = {
      val a = sc.parallelize(1 to 10, 2)
      val glomValue: RDD[Array[Int]] = a.glom
      val arr: Array[Array[Int]] = glomValue.collect()
      arr.foreach(x=>{
        x.foreach(y=>print(y + " "))
        println()
      })

      /**
       * 1 2 3 4 5
       * 6 7 8 9 10
       */
    }


    /**
     *  5.union
     *  UNION指将两个RDD中的数据集进行合并，最终返回两个RDD的并集，如RDD中存在相同的元素，也不会去重。
     */
    def testUnion(): Unit = {
      val a =sc.parallelize(1 to 3, 1)
      var b = sc.parallelize(1 to 7, 1)
      val ints: Array[Int] = a.union(b).collect()
      ints.foreach(y=>print(y + " "))

      /**
       * 1 2 3 1 2 3 4 5 6 7
       */
    }


    /**
     * 6.cartesian
     * 对两个RDD中所有元素进行笛卡尔积操作。
     */
    def testCartesian(): Unit = {
      val x = sc.parallelize((1 to 3).toList)
      val y = sc.parallelize((6 to 9).toList)
      val rdd: RDD[(Int, Int)] = x.cartesian(y)
      val tuples: Array[(Int, Int)] = rdd.collect()
      tuples.foreach(y=>print(y + " "))

      /**
       * (1,6) (1,7) (1,8) (1,9) (2,6) (2,7) (2,8) (2,9) (3,6) (3,7) (3,8) (3,9)
       */
    }

    /**
     * 7.groupBy
     * 生成相应的key，相同的放在一起even(2,4,6,8)
     */
    def testGroupBy(): Unit = {
      val a = sc.parallelize(1 to 9, 3)
      val rdd: RDD[(String, Iterable[Int])] = a.groupBy(x => {if (x%2 == 0) "even" else "odd"})
      val tuples: Array[(String, Iterable[Int])] = rdd.collect()
      tuples.foreach(x=>{
        print(x._1 + ":")
        x._2.foreach(y=>print(y + " "))
        println()
      })

      /**
       * even:2 4 6 8
       * odd:1 3 5 7 9
       */
    }

    /**
     * 8.filter
     * 对元素进行过滤，对每个元素进行f函数，返回值为true的元素在RDD中保留
     */
    def testFilter(): Unit = {
      val a = sc.parallelize(1 to 10, 3)
      val b = a.filter( _ % 2==0)
      val ints: Array[Int] = b.collect()
      ints.foreach(y=>print(y + " "))

      /**
       * 2 4 6 8 10
       */
    }

    /**
     * 9.distinct
     * distinct用于去重
     */
    def testDistinct(): Unit = {
      val a = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  2)
      val strings: Array[String] = a.distinct().collect()
      strings.foreach(y=>print(y + " "))

      /**
       * Dog Cat Gnu Rat
       */
    }


    /**
     * 10.subtract
     * 去掉含有重复的项
     */

    def testSubtract(): Unit = {
      val a = sc.parallelize(1 to 9, 3)
      val b = sc.parallelize(1 to 3, 3)
      val c: RDD[Int] = a.subtract(b)
      val ints: Array[Int] = c.collect()
      ints.foreach(y=>print(y + " "))

      /**
       * 6 9 4 7 5 8
       */
    }


    /**
     * 11.sample
     * 以指定的随机种子随机抽样出数量为fraction的数量，withReplacement表示抽出的数据是否放回，true为有放回抽样，false为无放回抽样。
     */
    def testSample(): Unit = {
      val a = sc.parallelize(1 to 100, 3)
      val rdd: RDD[Int] = a.sample(false, 0.1, 0)
      val ints: Array[Int] = rdd.collect()
      ints.foreach(y=>print(y + " "))

      /**
       * 10 47 55 73 76 84 87 88 91 92 95 98
       */
    }

    /**
     * 12.takesample
     * takesample与sample函数是一个原理，但是不使用相对比例采样，而是按设定的采样个数进行采样，
     * 同时返回的数据不再是RDD，而是相当于采样后的数据进行collect()，返回结果的集合为单机的数组。
     */
    def testTakesample(): Unit = {
      val x = sc.parallelize(1 to 1000, 3)
      val ints: Array[Int] = x.takeSample(true, 10, 1)
      ints.foreach(y=>print(y + " "))

      /**
       * 630 743 715 404 700 568 822 222 854 586
       */
    }

    /**
     * 13.cache、persist
     * cache和persist都是将一个RDD进行缓存，这样之后使用的过程中就不需要重新计算了，就可以大大节省程序运行时间。
     */
    def testCache_Persist(): Unit = {
      val c =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  2)
      println(c.getStorageLevel)
      c.cache()
      println(c.getStorageLevel)

      /**
       * StorageLevel(1 replicas)
       * StorageLevel(memory, deserialized, 1 replicas)
       */
    }

  }
}
