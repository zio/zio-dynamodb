package zio.dynamodb.benchmarks.blocks

import dynosaur.Schema.WriteError
import dynosaur.{ DynamoValue => DynosaurValue }
import org.openjdk.jmh.annotations._
import org.scanamo.DynamoReadError.describe
import org.scanamo.{ DynamoValue => ScanamoValue }
import zio.blocks.schema.Schema
import zio.dynamodb.blocks.DummyCodec2.AttributeValue2
import zio.dynamodb.blocks.{ DummyCodec2, DynamoDbCodec2 }

/**
 * borrows heavily from Andriy Plokhotnyuk's zio-blocks benchmarks https://github.com/zio/zio-blocks
 */
class ListOfRecordsBenchmark2 extends BaseBenchmark {
  import ListOfRecordsDomain._

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int                                            = 1
  var listOfRecords: List[Person]                          = _
  var encodedListOfRecords: List[AttributeValue2]          = _
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
//          map = Map("key1" -> 1, "key2" -> 2, "key3" -> 3)
//          List(5, 7, 9)
//          paymentMethod = PaymentMethod.CreditCard("John", 123)
        )
      )
      .toList
    encodedListOfRecords = listOfRecords.map(zioBlocksCodec2.encoder(_))
    encodedListOfRecordsForDynosaur = listOfRecords.map { x =>
      DynosaurSchema.personSchema.write(x).getOrElse(throw new Exception("Failed to encode"))
    }
    encodedListOfRecordsForScanamo = listOfRecords.map { x =>
      ScanamoCodec.person.write(x)
    }
  }

//  @Benchmark
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

//  @Benchmark
  def readingZioBlocks: List[Person] =
    encodedListOfRecords.map(av =>
      zioBlocksCodec2.decoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def writingScanamo: Seq[ScanamoValue] = listOfRecords.map(ScanamoCodec.person.write)

//  @Benchmark
  def writingDynosaur: Seq[Either[WriteError, DynosaurValue]] = listOfRecords.map(DynosaurSchema.personSchema.write)

  @Benchmark
  def writingZioBlocks: Seq[AttributeValue2] = listOfRecords.map(zioBlocksCodec2.encoder(_))

  val zioBlocksCodec2: DynamoDbCodec2[Person] = Schema.derived.deriving(DummyCodec2.DummyDeriver).derive

}
