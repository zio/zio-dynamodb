package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, Codec }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.{ equalTo, isRight }
import zio.test._

import java.math.BigInteger
import scala.collection.immutable.Map

object SetCodecSpec extends DefaultRunnableSpec {

  final case class HasBigDecimalSet(set: Set[BigDecimal])
  object HasBigDecimalSet        {
    implicit val schema: Schema[HasBigDecimalSet] = DeriveSchema.gen[HasBigDecimalSet]
  }
  final case class HasJavaBigDecimalSet(set: Set[java.math.BigDecimal])
  object HasJavaBigDecimalSet    {
    implicit val schema: Schema[HasJavaBigDecimalSet] = DeriveSchema.gen[HasJavaBigDecimalSet]
  }
  final case class HasBigIntSet(set: Set[BigInt])
  object HasBigIntSet            {
    implicit val schema: Schema[HasBigIntSet] = DeriveSchema.gen[HasBigIntSet]
  }
  final case class HasJavaBigIntegerSet(set: Set[java.math.BigInteger])
  object HasJavaBigIntegerSet    {
    implicit val schema: Schema[HasJavaBigIntegerSet] = DeriveSchema.gen[HasJavaBigIntegerSet]
  }
  final case class HasSetWithNonNativeType(set: Set[Boolean])
  object HasSetWithNonNativeType {
    implicit val schema: Schema[HasSetWithNonNativeType] = DeriveSchema.gen[HasSetWithNonNativeType]
  }

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] =
    suite("Set codecs")(
      suite("when encoding")(
        test("encodes set of BigDecimal natively") {

          val actual: AttributeValue =
            Codec.encoder(HasBigDecimalSet.schema)(
              HasBigDecimalSet(Set(BigDecimal(1), BigDecimal(2)))
            )

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1, 2))))"))
        },
        test("encodes set of Java BigDecimal natively") {
          val actual: AttributeValue =
            Codec.encoder(HasJavaBigDecimalSet.schema)(
              HasJavaBigDecimalSet(Set(new java.math.BigDecimal(1), new java.math.BigDecimal(2)))
            )

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1.0, 2.0))))"))
        },
        test("encodes set of BigInt natively") {

          val actual: AttributeValue =
            Codec.encoder(HasBigIntSet.schema)(
              HasBigIntSet(Set(BigInt(1), BigInt(2)))
            )

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1, 2))))"))
        },
        test("encodes set of Java BigInteger natively") {

          val actual: AttributeValue =
            Codec.encoder(HasJavaBigIntegerSet.schema)(
              HasJavaBigIntegerSet(Set(BigInteger.valueOf(1), BigInteger.valueOf(2)))
            )

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1, 2))))"))
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
        test("decodes set of BigDecimal natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
          )

          val actual = Codec.decoder(HasBigDecimalSet.schema)(av)

          assert(actual)(isRight(equalTo(HasBigDecimalSet(Set(BigDecimal(1), BigDecimal(2))))))
        },
        test("decodes set of Java BigDecimal natively") {
          val av = AttributeValue.Map(
            Map(
              AttributeValue.String("set") -> AttributeValue.NumberSet(
                Set(new java.math.BigDecimal(1), new java.math.BigDecimal(2))
              )
            )
          )

          val actual = Codec.decoder(HasJavaBigDecimalSet.schema)(av)

          assert(actual)(
            isRight(equalTo(HasJavaBigDecimalSet(Set(new java.math.BigDecimal(1), new java.math.BigDecimal(2)))))
          )
        },
        test("decodes set of BigInt natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
          )

          val actual = Codec.decoder(HasBigIntSet.schema)(av)

          assert(actual)(isRight(equalTo(HasBigIntSet(Set(BigInt(1), BigInt(2))))))
        },
        test("decodes set of Java BigInteger natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
          )

          val actual = Codec.decoder(HasJavaBigIntegerSet.schema)(av)

          assert(actual)(isRight(equalTo(HasJavaBigIntegerSet(Set(BigInteger.valueOf(1), BigInteger.valueOf(2))))))
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
