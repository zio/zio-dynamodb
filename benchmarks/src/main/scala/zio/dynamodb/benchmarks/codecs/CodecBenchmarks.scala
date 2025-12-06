package zio.dynamodb.benchmarks.codecs

import org.scanamo.{ DynamoValue => ScanamoValue }
import dynosaur.{ DynamoValue => DynosaurValue }
import dynosaur.Schema.WriteError
import org.openjdk.jmh.annotations._
import org.scanamo.DynamoReadError.describe
import zio.dynamodb.AttributeValue
import zio.dynamodb.{ Codec, Decoder, Encoder }
import zio.schema.{ DeriveSchema, Schema => ZIOSchema }

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks benchmarks https://github.com/zio/zio-blocks
 */
class CodecBenchmarks extends BaseBenchmark {
  import BenchmarkDomain._

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int                                            = 1
  var listOfRecords: List[Person]                          = _
  var encodedListOfRecords: List[AttributeValue]           = _
  var encodedListOfRecordsForDynosaur: List[DynosaurValue] = _
  var encodedListOfRecordsForScanamo: List[ScanamoValue]   = _

  @Setup
  def setup(): Unit = {
    listOfRecords = (1 to size)
      .map(_ =>
        Person(
          12345678901L,
          "John",
          30,
          "123 Main St"
        )
      )
      .toList

    encodedListOfRecords = listOfRecords.map(zioSchemaEncoder(_))
//    println(s"XXXXXX setup encodedListOfRecords: ${encodedListOfRecords}")
    encodedListOfRecordsForDynosaur = listOfRecords.map { x =>
      DynosaurSchema.personSchema.write(x).getOrElse(throw new Exception("Failed to encode"))
    }
    encodedListOfRecordsForScanamo = listOfRecords.map { x =>
      ScanamoCodec.person.write(x)
    }
  }

  @Benchmark
  def readingScanamo: List[Person] =
    encodedListOfRecordsForScanamo.map(av =>
      ScanamoCodec.person.read(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(describe(error))
      }
    )

//  @Benchmark
  def readingDynosaur: List[Person] =
    encodedListOfRecordsForDynosaur.map(av =>
      DynosaurSchema.personSchema.read(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def readingZioSchema: List[Person] =
    encodedListOfRecords.map(av =>
      zioSchemaDecoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

//  @Benchmark
  def writingScanamo: Seq[ScanamoValue] = listOfRecords.map(ScanamoCodec.person.write)

  //@Benchmark
  def writingDynosaur: Seq[Either[WriteError, DynosaurValue]] = listOfRecords.map(DynosaurSchema.personSchema.write)

//  @Benchmark
  def writingZioSchema: Seq[AttributeValue] = listOfRecords.map(zioSchemaEncoder(_))

}

object BenchmarkDomain {
  case class Person(
    id: Long,
    name: String,
    age: Int,
    address: String
  )
  object Person {}

  val zioSchema: ZIOSchema[Person] = DeriveSchema.gen[Person]

  val zioSchemaEncoder: Encoder[Person] = Codec.encoder[Person](zioSchema)
  val zioSchemaDecoder: Decoder[Person] = Codec.decoder[Person](zioSchema)

}
