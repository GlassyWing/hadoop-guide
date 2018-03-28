import java.nio.file.{Files, Path, StandardCopyOption}

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.Map

class WordCountHistogramTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override protected def beforeEach(): Unit = {
    sc = new SparkContext("local", "test")
  }

  override protected def afterEach(): Unit = {
    sc.stop()
  }

  test("word count histogram") {
    val input: Path = Files.createTempFile("input", "")
    Files.copy(getClass.getResourceAsStream("set2.txt"), input, StandardCopyOption.REPLACE_EXISTING)
    val hist: Map[Int, Long] = sc.textFile(input.toString)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
      .map(_.swap)
      .countByKey()
    assert(hist.size === 2)
    assert(hist(1) === 3)
    assert(hist(2) === 1)
  }

}
