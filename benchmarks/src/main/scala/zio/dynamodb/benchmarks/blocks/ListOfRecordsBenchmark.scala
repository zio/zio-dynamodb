package zio.dynamodb.benchmarks.blocks

import org.openjdk.jmh.annotations._
import zio.dynamodb.AttributeValue
//import zio.Chunk
import zio.blocks.schema.Schema
import zio.dynamodb.{ Codec, Decoder, Encoder }
import zio.dynamodb.blocks.{ BlocksDdbDerived, DdbCodec }
import zio.schema.{ DeriveSchema, Schema => ZIOSchema }

// sbt "zio-dynamodb-benchmarks/jmh:run ListOfRecordsBenchmark"
class ListOfRecordsBenchmark extends BaseBenchmark {
  import ListOfRecordsDomain._

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int                                  = 1000
  var listOfRecords: List[Person]                = _
  var encodedListOfRecords: List[AttributeValue] = _

  @Setup
  def setup(): Unit = {
    listOfRecords = (1 to size).map(_ => Person(12345678901L, "John", 30, "123 Main St", List(5, 7, 9))).toList
    encodedListOfRecords = listOfRecords.map(zioBlocksCodec.encoder(_))
  }

  @Benchmark
  def readingZioBlocks: List[Person] =
    encodedListOfRecords.map(av =>
      zioBlocksCodec.decoder(av) match {
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

  @Benchmark
  def writingZioBlocks: Seq[AttributeValue] = listOfRecords.map(zioBlocksCodec.encoder(_))

  @Benchmark
  def writingZioSchema: Seq[AttributeValue] = listOfRecords.map(zioSchemaEncoder(_))

}

object ListOfRecordsDomain {
  case class Person(id: Long, name: String, age: Int, address: String, childrenAges: List[Int])

  val zioSchema: ZIOSchema[Person] = DeriveSchema.gen[Person]

  val zioSchemaEncoder: Encoder[Person] = Codec.encoder[Person](zioSchema)
  val zioSchemaDecoder: Decoder[Person] = Codec.decoder[Person](zioSchema)

  val zioBlocksCodec: DdbCodec[Person] = Schema.derived.deriving(BlocksDdbDerived).derive
}
