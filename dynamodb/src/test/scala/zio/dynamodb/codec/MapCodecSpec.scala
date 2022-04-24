package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, Codec }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.{ equalTo, isRight }
import zio.test._
import zio.test.ZIOSpecDefault

// TODO: extending with CodecTestFixtures leads to map with string key implicit being used throughout the spec
object MapCodecSpec extends ZIOSpecDefault {

  final case class HasMapWithStringKey(map: Map[String, Int])
  object HasMapWithStringKey {
    implicit val schema: Schema[HasMapWithStringKey] = DeriveSchema.gen[HasMapWithStringKey]
  }

  final case class HasMapWithNonStringKey(map: Map[Int, Int])
  object HasMapWithNonStringKey {
    implicit val schema: Schema[HasMapWithNonStringKey] = DeriveSchema.gen[HasMapWithNonStringKey]
  }

  override def spec: ZSpec[zio.test.TestEnvironment, Any] =
    suite("Map codecs")(
      suite("when encoding")(
        test("encodes map with string key natively") {
          val actual: AttributeValue = Codec.encoder(HasMapWithStringKey.schema)(HasMapWithStringKey(Map("1" -> 1)))

          assert(actual.toString)(equalTo("Map(Map(String(map) -> Map(Map(String(1) -> Number(1)))))"))
        },
        test("encodes map with non string key as sequence of Tuple2") {
          val actual: AttributeValue = Codec.encoder(HasMapWithNonStringKey.schema)(HasMapWithNonStringKey(Map(1 -> 1)))

          assert(actual.toString)(
            equalTo("Map(Map(String(map) -> List(Chunk(List(Chunk(Number(1),Number(1)))))))")
          )
        }
      ),
      suite("when decoding")(
        test("decodes map with string key natively") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("map") -> AttributeValue.Map(
                Map(AttributeValue.String("one") -> AttributeValue.Number(BigDecimal(1)))
              )
            )
          )

          val actual = Codec.decoder(HasMapWithStringKey.schema)(av)

          assert(actual)(isRight(equalTo(HasMapWithStringKey(Map("one" -> 1)))))
        },
        test("decodes map with non string key") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("map") -> AttributeValue.List(
                List(
                  AttributeValue.List(List(AttributeValue.Number(BigDecimal(1)), AttributeValue.Number(BigDecimal(1))))
                )
              )
            )
          )

          val actual = Codec.decoder(HasMapWithNonStringKey.schema)(av)

          assert(actual)(isRight(equalTo(HasMapWithNonStringKey(Map(1 -> 1)))))
        }
      )
    )
}
