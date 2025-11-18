package zio.dynamodb.benchmarks.blocks

import org.scanamo.{ DynamoValue => ScanamoValue }
import dynosaur.{ DynamoValue => DynosaurValue }
import dynosaur.Schema.WriteError
import org.openjdk.jmh.annotations._
import org.scanamo.DynamoReadError.describe
import zio.dynamodb.AttributeValue
import zio.blocks.schema.{ CompanionOptics, Schema }
import zio.dynamodb.{ Codec, Decoder, Encoder }
import zio.dynamodb.blocks.{ BlocksDdbDerived2, DdbCodec }
import zio.schema.{ DeriveSchema, Schema => ZIOSchema }

class ListOfRecordsBenchmark extends BaseBenchmark {
  import ListOfRecordsDomain._

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int                                            = 1000
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
//          List(5, 7, 9)
//          paymentMethod = PaymentMethod.CreditCard("John", 123)
        )
      )
      .toList
    encodedListOfRecords = listOfRecords.map(zioBlocksCodec.encoder(_))
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
      zioBlocksCodec.decoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

//  @Benchmark
  def readingZioSchema: List[Person] =
    encodedListOfRecords.map(av =>
      zioSchemaDecoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def writingScanamo: Seq[ScanamoValue] = listOfRecords.map(ScanamoCodec.person.write)

//  @Benchmark
  def writingDynosaur: Seq[Either[WriteError, DynosaurValue]] = listOfRecords.map(DynosaurSchema.personSchema.write)

  @Benchmark
  def writingZioBlocks: Seq[AttributeValue] = listOfRecords.map(zioBlocksCodec.encoder(_))

//  @Benchmark
  def writingZioSchema: Seq[AttributeValue] = listOfRecords.map(zioSchemaEncoder(_))

}

object ListOfRecordsDomain {
  sealed trait PaymentMethod
  object PaymentMethod extends CompanionOptics[PaymentMethod] {
    case class CreditCard(name: String, cvv: Int) extends PaymentMethod
    object CreditCard {
      implicit val zioSchema: ZIOSchema[CreditCard] = DeriveSchema.gen[CreditCard]

      implicit val blocksSchema: Schema[CreditCard] = Schema.derived
    }
    case object DebitCard extends PaymentMethod
    case object Paypal extends PaymentMethod

    implicit val zioSchema: ZIOSchema[PaymentMethod] = DeriveSchema.gen[PaymentMethod]

    implicit val blocksSchema: Schema[PaymentMethod] = Schema.derived
  }
  case class Person(
    id: Long,
    name: String,
    age: Int,
    address: String
//    childrenAges: List[Int]
//    paymentMethod: PaymentMethod
  )

  val zioSchema: ZIOSchema[Person] = DeriveSchema.gen[Person]

  val zioSchemaEncoder: Encoder[Person] = Codec.encoder[Person](zioSchema)
  val zioSchemaDecoder: Decoder[Person] = Codec.decoder[Person](zioSchema)

  val zioBlocksCodec: DdbCodec[Person] = Schema.derived.deriving(BlocksDdbDerived2).derive
}
