import java.io.File

import com.google.common.io.{Files, Resources}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.DatumWriter
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FunSuite}
import specific.WeatherRecord

class RDDCreationTest extends FunSuite with BeforeAndAfterEach {

  var sc: SparkContext = _

  override def beforeEach(): Unit = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "CustomKryoRegistrator")
    sc = new SparkContext("local", "test", conf)
  }

  override def afterEach(): Unit = {
    sc.stop()
  }

  test("parallelized collection") {
    val  performExpensiveComputation = (x: Int) => x * (x + 1) /2

    val params = sc.parallelize(1 to 10)
    val result = params.map(performExpensiveComputation)

    assert(result.collect === (1 to 10).map(performExpensiveComputation))
  }

  test("text file") {
    val input: File = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")),
      input)
    val text: RDD[String] = sc.textFile(input.getPath)
    assert(text.collect().toList === List("cherry", "apple", "banana"))
  }

  test("sequence file writable") {
    val input = File.createTempFile("input", "")
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("numbers.seq")),
      input)
    val data = sc.sequenceFile[IntWritable, Text](input.getPath)
    assert(data.first()._1 === new IntWritable(100))
    assert(data.first()._2 === new Text("One, two, buckle my shoe"))
  }

  test("avro generic data file") {
    val  inputPath = "target/data.avro"
    val avroSchema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "StringPair",
        |  "doc": "A pair of strings.",
        |  "fields": [
        |    {"name": "left", "type": "string"},
        |    {"name": "right", "type": "string"}
        |  ]
        |}""".stripMargin)

    val datum: GenericRecord = new GenericData.Record(avroSchema)
    datum.put("left", "L")
    datum.put("right", "R")

    val file: File = new File(inputPath)
    val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](avroSchema)
    val dataFileWriter: DataFileWriter[GenericRecord] = new DataFileWriter[GenericRecord](writer)

    dataFileWriter.create(avroSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close()

    val  job = Job.getInstance()
    AvroJob.setInputKeySchema(job, avroSchema)
    val data = sc.newAPIHadoopFile(inputPath,
      classOf[AvroKeyInputFormat[GenericRecord]],
      classOf[AvroKey[GenericRecord]], classOf[NullWritable],
      job.getConfiguration)

    val record = data.first()._1.datum()
    println(record)
    assert(record.get("left").toString === "L" )
    assert(record.get("right").toString === "R")

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first()._1.datum()
    assert(record2.get("left").toString === "L")
    assert(record2.get("right").toString === "R")
  }

  test("avro specific data file") {
    val  inputPath = "target/data.avro"
    val datum = new WeatherRecord(2000, 10, "id")

    val file:File = new File(inputPath)
    val writer: DatumWriter[WeatherRecord] = new SpecificDatumWriter[WeatherRecord]()
    val dataFileWriter: DataFileWriter[WeatherRecord] = new DataFileWriter[WeatherRecord](writer)
    dataFileWriter.create(WeatherRecord.getClassSchema, file)
    dataFileWriter.append(datum)
    dataFileWriter.close()

    val job = Job.getInstance()
    AvroJob.setInputKeySchema(job, WeatherRecord.getClassSchema)
    val data = sc.newAPIHadoopFile(inputPath
      , classOf[AvroKeyInputFormat[WeatherRecord]]
      , classOf[AvroKey[WeatherRecord]]
      , classOf[NullWritable]
      , job.getConfiguration)

    val record = data.first()._1.datum()
    assert(record === datum)

    // try to do a map to see if the Avro record can be serialized
    val record2 = data.map(rec => rec).first._1.datum
    assert(record2 === datum)


  }

}
