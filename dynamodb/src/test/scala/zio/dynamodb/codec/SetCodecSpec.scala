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

  final case class HasSetWithNonNativeType(set: Set[Boolean])
  object HasSetWithNonNativeType {
    implicit val schema: Schema[HasSetWithNonNativeType] = DeriveSchema.gen[HasSetWithNonNativeType]
  }

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] =
    suite("Set codecs")(
      suite("when encoding")(
        test("encodes set of string natively") {
          val actual: AttributeValue = Codec.encoder(HasStringSet.schema)(HasStringSet(Set("1", "2")))

          assert(actual.toString)(equalTo("Map(Map(String(set) -> StringSet(Set(1, 2))))"))
        },
        test("encodes set with non native type as a list") {
          val actual: AttributeValue =
            Codec.encoder(HasSetWithNonNativeType.schema)(HasSetWithNonNativeType(Set(true, false)))

          assert(actual.toString)(
            equalTo("Map(Map(String(set) -> List(Chunk(Bool(true),Bool(false)))))")
          )
        }
      ),
      suite("when decoding")(
        test("decodes set of string natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.StringSet(Set("one", "two")))
          )

          val actual = Codec.decoder(HasStringSet.schema)(av)

          assert(actual)(isRight(equalTo(HasStringSet(Set("one", "two")))))
        },
        test("decodes set with non native type") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("set") -> AttributeValue.List(
                List(AttributeValue.Bool(true), AttributeValue.Bool(false))
              )
            )
          )

          val actual = Codec.decoder(HasSetWithNonNativeType.schema)(av)

          assert(actual)(isRight(equalTo(HasSetWithNonNativeType(Set(true, false)))))
        }
      )
    )
}
