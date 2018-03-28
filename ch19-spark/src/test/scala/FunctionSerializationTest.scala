import org.apache.spark.{SparkContext, SparkException}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class FunctionSerializationTest extends FunSuite with BeforeAndAfterEach{

  var sc: SparkContext = _

  override protected def beforeEach(): Unit = {
    sc = new SparkContext("local", "test")
  }

  override protected def afterEach(): Unit = {
    sc.stop()
  }

  test("serializable anonymous function") {
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce(_ + _)
    assert(result === 6)
  }

  test("serializable local function") {
    val myAddFn = (x: Int, y: Int) => x + y
    val params = sc.parallelize(Array(1,2 ,3))
    val result = params.reduce(myAddFn)
    assert(result === 6)
  }

  test("serializable global singleton function") {
    val params = sc.parallelize(Array(1 ,3 ,2))
    val result = params.reduce(NonSerializableSingleton.myAddFn)
    assert(result === 6)
  }

  test("non-serializable class function") {
    val nonSerializable = new NonSerializableClass()
    val params = sc.parallelize(Array(1, 2, 3))
    intercept[SparkException] {
      params.reduce(nonSerializable.myAddFn)
    }
  }

  test("serializable class function") {
    val serializable = new SerializableClass()
    val params = sc.parallelize(Array(1, 2, 3))
    val result = params.reduce(serializable.myAddFn)
    assert(result === 6)
  }

}

object NonSerializableSingleton {
  def myAddFn(x: Int, y: Int): Int = {x + y}
}

class NonSerializableClass {
  def myAddFn(x: Int, y: Int): Int = {x + y}
}

class SerializableClass extends Serializable {
  def myAddFn(x: Int, y: Int):Int = {x + y}
}
