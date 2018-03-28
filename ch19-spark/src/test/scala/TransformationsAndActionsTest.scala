import java.io.File
import java.nio.file.attribute.FileAttribute
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable
import scala.reflect.ClassTag

class TransformationsAndActionsTest extends FunSuite with BeforeAndAfterEach{

  var sc: SparkContext = _

  override def beforeEach(): Unit ={
    sc = new SparkContext("local", "test")
  }

  override def afterEach(): Unit = {
    sc.stop()
  }

  test("lazy transformations") {
    val input: Path = Files.createTempFile("input", "")
    Files.copy(getClass.getResourceAsStream("fruit.txt"), input, StandardCopyOption.REPLACE_EXISTING)
    val text = sc.textFile(input.toString)
    val lower: RDD[String] = text.map(_.toLowerCase())
    println("Called toLowerCase")
    lower.foreach(println)
    input.toFile.deleteOnExit()
  }

  test("map reduce") {
    val input: Path = Files.createTempFile("input", "")
    input.toFile.deleteOnExit()
    Files.copy(getClass.getResourceAsStream("quangle.txt"), input, StandardCopyOption.REPLACE_EXISTING)
    val text: RDD[String] = sc.textFile(input.toString)

    val in: RDD[(String, Int)] = text.flatMap(_.split(" ")).map(word => (word, 1))

    val  mapFn = (kv: (String, Int)) => List(kv)
    val reduceFn = (kv: (String, Iterable[Int])) => List((kv._1, kv._2.sum))
    new MapReduce[String, Int, String, Int, String, Int]()
      .naiveMapReduce(in, mapFn, reduceFn).foreach(println)
  }

  test("reduceByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("b", 7), ("a", 1), ("a", 5)))
    val sums: RDD[(String, Int)] = pairs.foldByKey(0)(_ + _)
    assert(sums.collect().toSet === Set(("a", 9), ("b", 7)))
  }

  test("aggregateByKey") {
    val pairs: RDD[(String, Int)] =
      sc.parallelize(Array(("a", 3), ("b", 7), ("a", 1), ("a", 5)))
    val sets: RDD[(String, mutable.HashSet[Int])] =
      pairs.aggregateByKey(new mutable.HashSet[Int])(_+=_,  _++=_)
    assert(sets.collect().toSet === Set(("a", Set(1, 3, 5)), ("b", Set(7))))
  }

}

class MapReduce[K1, V1, K2, V2, K3, V3] {

  def naiveMapReduce[K2: Ordering: ClassTag, V2:ClassTag]
  (input:RDD[(K1, V1)]
   , mapFn: ((K1, V1)) => TraversableOnce[(K2, V2)]
   , reduceFn: ((K2, Iterable[V2])) => TraversableOnce[(K3, V3)]):
  RDD[(K3, V3)] = {
    val mapOutput: RDD[(K2, V2)] = input.flatMap(mapFn)
    val shuffled: RDD[(K2, Iterable[V2])] = mapOutput.groupByKey().sortByKey()
    val output: RDD[(K3, V3)] = shuffled.flatMap(reduceFn)
    output
  }

}