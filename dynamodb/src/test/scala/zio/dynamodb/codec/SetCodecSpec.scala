package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, Codec }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.{ equalTo, isRight }
import zio.test._

object SetCodecSpec extends DefaultRunnableSpec {

  final case class HasStringSet(set: Set[String])
  object HasStringSet {
    implicit val schema: Schema[HasStringSet] = DeriveSchema.gen[HasStringSet]
  }

  final case class HasSetWithNonNativeType(set: Set[Double])
  object HasSetWithNonNativeType {
    implicit val schema: Schema[HasSetWithNonNativeType] = DeriveSchema.gen[HasSetWithNonNativeType]
  }

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] =
    suite("Set codecs")(
      suite("when encoding")(
        test("encodes set with string key natively") {
//          val actual: AttributeValue = Codec.encoder(HasStringSet.schema)(HasStringSet(Map("1" -> 1)))
//
//          assert(actual.toString)(equalTo("Map(Map(String(map) -> Map(Map(String(1) -> Number(1)))))"))
          assertCompletes
        },
        test("encodes set with non native type as a sequence") {
          val actual: AttributeValue =
            Codec.encoder(HasSetWithNonNativeType.schema)(HasSetWithNonNativeType(Set(0.1, 0.2)))

          assert(actual.toString)(
            equalTo("Map(Map(String(set) -> List(Chunk(Number(0.1),Number(0.2)))))")
          )
        }
      ),
      suite("when decoding")(
        test("decodes set with string key natively") {
//          val av = AttributeValue.Map(
//            Map(
//              AttributeValue.String("map") -> AttributeValue.Map(
//                Map(AttributeValue.String("one") -> AttributeValue.Number(BigDecimal(1)))
//              )
//            )
//          )
//
//          val actual = Codec.decoder(HasStringSet.schema)(av)
//
//          assert(actual)(isRight(equalTo(HasStringSet(Map("one" -> 1)))))
          assertCompletes
        },
        test("decodes map with non native type") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("set") -> AttributeValue.List(
                List(
                  AttributeValue.Number(BigDecimal(0.1)),
                  AttributeValue.Number(BigDecimal(0.2))
                )
              )
            )
          )

          val actual = Codec.decoder(HasSetWithNonNativeType.schema)(av)

          assert(actual)(isRight(equalTo(HasSetWithNonNativeType(Set(0.1, 0.2)))))
        }
      )
    )
}
