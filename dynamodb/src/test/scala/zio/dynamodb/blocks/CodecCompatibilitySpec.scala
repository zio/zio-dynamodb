package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Schema }
import zio.dynamodb.{ Item, SchemaCodec }
import zio.test.{ assertTrue, TestResult, ZIOSpecDefault }

object CodecCompatibilitySpec extends ZIOSpecDefault {
  final case class Person2(id: String, age: Int, count: Long)
  object Person2 extends CompanionOptics[Person2] {
    implicit val blocksSchema: Schema[Person2]         = Schema.derived
    implicit val zioSchema: zio.schema.Schema[Person2] = zio.schema.DeriveSchema.gen[Person2]
  }

  def withCodecs[A](zioSchema: zio.schema.Schema[A], blocks: Schema[A])(
    testBody: SchemaCodec[A] => TestResult
  ): TestResult = {
    val scBlocks: SchemaCodec[A] = SchemaCodec.schema2ToSchemaCodec(blocks, DynamoDBCodecDeriverConfigure.default)
    val scZio: SchemaCodec[A]    = SchemaCodec.schema1ToSchemaCodec(zioSchema)

    testBody(scBlocks) && testBody(scZio)
  }

  val spec = suite("BlocksCodecSpec2")(
    test("round trip Person2") {
      withCodecs(Person2.zioSchema, Person2.blocksSchema) { (codec: SchemaCodec[Person2]) =>
        val expectedItem   = Item("id" -> "1", "age" -> 42, "count" -> 100)
        val expectedPerson = Person2("1", 42, 100)
        val enc            = codec.encoder(expectedPerson)
        val dec            = codec.decoder(enc)
        assertTrue(enc == expectedItem.toAttributeValue && dec == Right(expectedPerson))
      }
    }
  )

}
