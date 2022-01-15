package zio.dynamodb.codec

import zio.Chunk
import zio.dynamodb.{ AttributeValue, Codec }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion.{ equalTo, hasKey, isRight }
import zio.test.AssertionM.Render.param
import zio.test._

import scala.collection.immutable.Map

object SetCodecSpec extends DefaultRunnableSpec {

  final case class HasStringSet(set: Set[String])
  object HasStringSet         {
    implicit val schema = DeriveSchema.gen[HasStringSet]
  }
  /*
  for `final case class HasBinarySet(set: Set[List[Byte]])` I get
  Failed to derive schema for Byte. Can only derive Schema for case class or sealed trait
   */
  final case class HasBinarySet(set: Set[Chunk[Byte]])
  object HasBinarySet         {
    implicit val schema = DeriveSchema.gen[HasBinarySet]
  }
  final case class HasIntSet(set: Set[Int])
  object HasIntSet            {
    implicit val schema: Schema[HasIntSet] = DeriveSchema.gen[HasIntSet]
  }
  final case class HasLongSet(set: Set[Long])
  object HasLongSet           {
    implicit val schema: Schema[HasLongSet] = DeriveSchema.gen[HasLongSet]
  }
  final case class HasDoubleSet(set: Set[Float])
  object HasDoubleSet         {
    implicit val schema: Schema[HasDoubleSet] = DeriveSchema.gen[HasDoubleSet]
  }
  final case class HasBigDecimalSet(set: Set[BigDecimal])
  object HasBigDecimalSet     {
    implicit val schema: Schema[HasBigDecimalSet] = DeriveSchema.gen[HasBigDecimalSet]
  }
  final case class HasJavaBigDecimalSet(set: Set[java.math.BigDecimal])
  object HasJavaBigDecimalSet {
    implicit val schema: Schema[HasJavaBigDecimalSet] = DeriveSchema.gen[HasJavaBigDecimalSet]
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
        test("encodes set of binary chunk natively") {
          val actual: AttributeValue =
            Codec.encoder(HasBinarySet.schema)(HasBinarySet(Set(Chunk(1.toByte), Chunk(2.toByte))))

          assert(actual)(
            equalTo(
              AttributeValue.Map(
                Map(AttributeValue.String("set") -> AttributeValue.BinarySet(Set(List(1.toByte), List(2.toByte))))
              )
            )
          )
        },
        test("encodes set of int natively") {
          val actual: AttributeValue = Codec.encoder(HasIntSet.schema)(HasIntSet(Set(1, 2)))

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1, 2))))"))
        },
        test("encodes set of long natively") {
          val actual: AttributeValue = Codec.encoder(HasLongSet.schema)(HasLongSet(Set(1, 2)))

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1, 2))))"))
        },
        test("encodes set of double") {
          val actual: AttributeValue = Codec.encoder(HasDoubleSet.schema)(HasDoubleSet(Set(0.0f)))

          assert(actual)(
            equalTo(
              AttributeValue.Map(
                Map(AttributeValue.String("set") -> AttributeValue.NumberSet(Set(0.0)))
              )
            )
          )
        },
        test("encodes set of BigDecimal natively") {
          /*
this = {Codec$Encoder$@2914}
s = {Schema$Transform@2915} "Transform(Primitive(bigDecimal,Chunk()))"
 codec = {Schema$Primitive@2923} "Primitive(bigDecimal,Chunk())"
  standardType = {StandardType$BigDecimalType$@2928} "bigDecimal"
   No fields to display
  annotations = {Chunk$Empty$@2926} "Chunk$Empty$" size = 0
 f = {Schema$lambda@2924}
 g = {Schema$lambda@2925}
 annotations = {Chunk$Empty$@2926} "Chunk$Empty$" size = 0
           */
          val s = HasBigDecimalSet.schema
          println(s"XXXXXXXXXXXXXXXXX s=$s")

          val actual: AttributeValue =
            Codec.encoder(HasBigDecimalSet.schema)(
              HasBigDecimalSet(Set(BigDecimal(1), BigDecimal(2)))
            )

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1, 2))))"))
        },
        test("encodes set of Java BigDecimal natively") {
          val actual: AttributeValue =
            Codec.encoder(HasJavaBigDecimalSet.schema)(
              HasJavaBigDecimalSet(Set(BigDecimal(1).bigDecimal, BigDecimal(2).bigDecimal))
            )

          actual match {
            case AttributeValue.Map(map) =>
              assert(map)(hasKey(AttributeValue.String("set"))) && {
                map.get(AttributeValue.String("set")) match {
                  case Some(AttributeValue.NumberSet(set)) =>
                    assert(set)(equalTo2(Set(BigDecimal(1), BigDecimal(2))))
                  case _                                   =>
                    assertTrue(false)
                }
              }
            case _                       =>
              assertTrue(false)
          }

          assert(actual.toString)(equalTo("Map(Map(String(set) -> NumberSet(Set(1.0, 2.0))))"))
//          assert(actual)(
//            equalTo2(
//              AttributeValue.Map(
//                Map(
//                  AttributeValue.String("set") -> AttributeValue.NumberSet(
//                    Set(BigDecimal(1), BigDecimal(2))
//                  )
//                )
//              )
//            )
//          )
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
        test("decodes binary set natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.BinarySet(Set(Chunk(1.toByte), Chunk(2.toByte))))
          )

          val actual = Codec.decoder(HasBinarySet.schema)(av)

          assert(actual)(isRight(equalTo(HasBinarySet(Set(Chunk(1.toByte), Chunk(2.toByte))))))
        },
        test("decodes set of int natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.NumberSet(Set(1, 2)))
          )

          val actual = Codec.decoder(HasIntSet.schema)(av)

          assert(actual)(isRight(equalTo(HasIntSet(Set(1, 2)))))
        },
        test("decodes set of long natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.NumberSet(Set(1L, 2L)))
          )

          val actual = Codec.decoder(HasLongSet.schema)(av)

          assert(actual)(isRight(equalTo(HasLongSet(Set(1L, 2L)))))
        },
        test("decodes set of double natively") {
          val av = AttributeValue.Map(
            Map(AttributeValue.String("set") -> AttributeValue.NumberSet(Set(0.0f)))
          )

          val actual = Codec.decoder(HasDoubleSet.schema)(av)

          assert(actual)(isRight(equalTo(HasDoubleSet(Set(0.0f)))))
        },
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

  def equalTo2[A, B](expected: A) /*(implicit eql: Eql[A, B])*/: Assertion[B] =
    Assertion.assertion("equalTo")(param(expected)) { actual =>
      (actual, expected) match {
        case (left: Set[a], right: Set[b])     =>
          println(s"left=$left right=$right")
          left.foldLeft[Int](0) {
            case (count, el) =>
              if (right.contains(el.asInstanceOf[b]))
                count + 1
              else
                count
          } == left.size
        case (left: Array[_], right: Array[_]) =>
          left.sameElements[Any](right)
        case (left, right)                     =>
          left == right
      }
    }
}
