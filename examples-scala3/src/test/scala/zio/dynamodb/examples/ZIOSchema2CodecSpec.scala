package zio.dynamodb.examples

import zio.blocks.schema.Schema
import zio.test.{ assertTrue, ZIOSpecDefault }

object ZIOSchema2CodecSpec extends ZIOSpecDefault {
  val spec = suite("ZIOSchema2CodecSpec")(
    test("basic round trip") {
      case class Person(id: String, name: String, age: Int)
      object Person {
        implicit val schema: Schema[Person] = Schema.derived
      }
      val codec = Person.schema.derive(zio.dynamodb.blocks.DynamoDBCodecDeriver)
      val person  = Person("1", "Alice", 30)
      val encoded = codec.encoder(person)
      val decoded = codec.decoder(encoded)

      assertTrue(decoded == Right(person))
    }
  )
}
