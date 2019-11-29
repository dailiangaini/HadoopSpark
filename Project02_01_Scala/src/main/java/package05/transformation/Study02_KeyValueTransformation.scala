package package05.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/28 16:30
 */
object Study02_KeyValueTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("StudySpark")
    val sc = new SparkContext(conf)

    /**
     * 1.mapValues
     * mapValues是针对[K,V]中V值进行map操作
     */
    def testMapValues(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  2)
      val b = a.map(x => (x.length,x))
      val rdd: RDD[(Int, String)] = b.mapValues("x" + _ + "x")
      val tuples: Array[(Int, String)] = rdd.collect()
      tuples.foreach(println)

      /**
       * (3,xGnux)
       * (3,xCatx)
       * (3,xRatx)
       * (3,xDogx)
       * (3,xGnux)
       */
    }

    /**
     * 2.combineByKey
     * 使用用户设置好的聚合函数对每个key中的value进行组合（combine）,可以将输入类型为RDD[(k,V)]转换成RDD[(k,c)]
     *
     * def combineByKey[C](
     * createCombiner: V => C,
     * mergeValue: (C, V) => C,
     * mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
     * /*content*/
     * }
     *
     *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
     * 这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
     *
     * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
     * 该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
     *
     *  - `mergeCombiners`, to combine two C's into a single one.
     * 该函数把2个元素C合并 (这个操作在不同分区间进行)s
     */
    def testCombineByKey(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  3)
      val b = sc.parallelize(List(1,1,2,2,3), 3)
      val c: RDD[(String, Int)] = a.zip(b)
      c.foreach(y=>println(y))

      /**
       * (Gnu,1)
       * (Cat,1)
       * (Rat,2)
       * (Dog,2)
       * (Gnu,3)
       */
      val rdd: RDD[(String, Int)] = c.combineByKey(
        (v: Int) => v,
        (v1: Int, v2: Int) => v1 + v2,
        (v4: Int, v3: Int) => v3 + v4
      )
      val tuples: Array[(String, Int)] = rdd.collect()
      tuples.foreach(println)

      /**
       * (Dog,2)
       * (Rat,2)
       * (Cat,1)
       * (Gnu,4)
       */
    }


    /**
     * 3.reduceByKey
     * 对元素为KV对的RDD中key相同的value进行value进行binary_function的reduce操作，
     * 因此，key相同的多个元素的值被reduce为一个值，然后与原RDD中的key组合成一个新的KV对。
     */
    def testReduceByKey(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"),  3)
      val b: RDD[(Int, String)] = a.map(x => (x.length, x))
      val result: RDD[(Int, String)] = b.reduceByKey(_+_)
      val result2: RDD[(Int, String)] = b.reduceByKey((x:String, y: String) => x +y)
      result.foreach(println)
      result2.foreach(println)

      /**
       * (3,GnuCatRatDogGnu)
       */
    }

    /**
     * 4.PartitionBy
     * 对RDD进行分区操作。
     */
    def testPartitionBy(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu"))
      val rdd: RDD[(String, Int)] = a.map(x => (x, x.length)).partitionBy(new HashPartitioner(2))
      rdd.foreachPartition(item => println(item.toList))

      /**
       * List((Gnu,3), (Cat,3), (Dog,3), (Gnu,3))
       * List((Rat,3))
       */
    }

    /**
     * 5.cogroup
     * cogroup指对两个RDD中的KV元素，每个RDD中相同的key中的元素分别聚合成一个集合。
     */
    def testCogroup(): Unit = {
      val rdd: RDD[Int] = sc.parallelize(List(1,2,1,3))
      var rdd2 = rdd.map((_, "b"))
      var rdd3 = rdd.map((_, "c"))
      val tuples: Array[(Int, (Iterable[String], Iterable[String]))] = rdd2.cogroup(rdd3).collect()
      tuples.foreach(item => {
        print(item._1, item._2.toString())
      })

      /**
       * (1,(CompactBuffer(b, b),CompactBuffer(c, c)))(3,(CompactBuffer(b),CompactBuffer(c)))(2,(CompactBuffer(b),CompactBuffer(c)))
       */
    }


    /**
     * 6.join
     * 对两个需要连接的RDD进行cogroup函数操作。
     */
    def testJoin(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu","elephant"),  2)
      val b: RDD[(Int, String)] = a.keyBy(_.length)

      val c: RDD[String] = sc.parallelize(List("dog", "salmon", "pig","bear"), 2)
      val d: RDD[(Int, String)] = c.keyBy(_.length)
      val tuples: Array[(Int, (String, String))] = b.join(d).collect()
      tuples.foreach(item => {
        print(item._1, item._2.toString())
      })

      /**
       * (3,(Gnu,dog))(3,(Gnu,pig))(3,(Cat,dog))(3,(Cat,pig))(3,(Rat,dog))(3,(Rat,pig))(3,(Dog,dog))(3,(Dog,pig))(3,(Gnu,dog))(3,(Gnu,pig))
       */
    }
    testJoin


    /**
     * 7.leftOutJoin
     */
    def testLeftOutJoin(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu","elephant"),  2)
      val b: RDD[(Int, String)] = a.keyBy(_.length)

      val c: RDD[String] = sc.parallelize(List("dog", "salmon", "pig","bear"), 2)
      val d: RDD[(Int, String)] = c.keyBy(_.length)
      val tuples: Array[(Int, (String, Option[String]))] = b.leftOuterJoin(d).collect()
      tuples.foreach(item => {
        print(item._1, item._2.toString())
      })

      /**
       * (8,(elephant,None))(3,(Gnu,Some(dog)))(3,(Gnu,Some(pig)))(3,(Cat,Some(dog)))(3,(Cat,Some(pig)))(3,(Rat,Some(dog)))(3,(Rat,Some(pig)))(3,(Dog,Some(dog)))(3,(Dog,Some(pig)))(3,(Gnu,Some(dog)))(3,(Gnu,Some(pig)))
       */
    }

    testLeftOutJoin

    /**
     * 8.rightOutJoin
     */
    def testRightOutJoin(): Unit = {
      val a =  sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu","elephant"),  2)
      val b: RDD[(Int, String)] = a.keyBy(_.length)

      val c: RDD[String] = sc.parallelize(List("dog", "salmon", "pig","bear"), 2)
      val d: RDD[(Int, String)] = c.keyBy(_.length)
      val tuples: Array[(Int, (Option[String], String))] = b.rightOuterJoin(d).collect()
      tuples.foreach(item => {
        print(item._1, item._2.toString())
      })

      /**
       * (4,(None,bear))(6,(None,salmon))(3,(Some(Gnu),dog))(3,(Some(Gnu),pig))(3,(Some(Cat),dog))(3,(Some(Cat),pig))(3,(Some(Rat),dog))(3,(Some(Rat),pig))(3,(Some(Dog),dog))(3,(Some(Dog),pig))(3,(Some(Gnu),dog))(3,(Some(Gnu),pig))
       */
    }
    testRightOutJoin
  }
}
