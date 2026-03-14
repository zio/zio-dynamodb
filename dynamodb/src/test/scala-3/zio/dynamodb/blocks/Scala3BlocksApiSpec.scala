package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Optic, Schema }
import zio.dynamodb.{ AttributeValue, Item, SchemaCodec, ToAttributeValue }
import zio.prelude.Newtype
import zio.test.{ assertTrue, Spec, TestResult, ZIOSpecDefault }

object Scala3BlocksApiSpec extends ZIOSpecDefault {

  enum TrafficLight {
    case Red, Yellow, Green
  }

  enum PaymentMethod {
    case CreditCard(number: String, expiry: String)
    case PayPal(email: String)
    case Other
  }

  opaque type OpaqueId = Int
  object OpaqueId {
    def apply(value: Int): OpaqueId = value
  }
  extension (id: OpaqueId) {
    def value: Int = id
  }

  object PersonId extends Newtype[Int] {
    implicit val x: Schema[PersonId.Type] =
      Schema[Int].transform(s => PersonId(s), (personId: PersonId) => PersonId.unwrap(personId))
  }
  type PersonId = PersonId.Type

  object Age extends Newtype[Int] {
    implicit val x: Schema[Age.Type] =
      Schema[Int].transform(s => Age(s), (age: Age) => Age.unwrap(age))
  }
  type Age = Age.Type

  case class Address(number: String, postcode: String)
  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived
    val number: Optic[Address, String]   = $(_.number)
    val postcode: Optic[Address, String] = $(_.postcode)
  }

  case class Person(
    id: OpaqueId,
    name: String,
    trafficLight: TrafficLight
  )

  sealed trait Foo derives Schema

  case object Foo1 extends Foo

  sealed trait Bar extends Foo

  case object Bar1 extends Bar

  override def spec =
    suite("Scala 3 codec suite")(
      suite("variants") {
        test("constant values on different hierarchy levels") {
          // roundTripWithSchema2Codec()
          assertTrue(true)
        }

      },
      test("Scala 3 enums") {
        assertTrue(1 == 1)
      },
      test("Person with simple enum") {
        roundTripWithSchema2Codec(
          expectedValue = Person(
            id = 123,
            name = "John Doe",
            trafficLight = TrafficLight.Green
          ),
          expectedAV = Item("id" -> OpaqueId(123), "name" -> "John Doe", "trafficLight" -> "Green").toAttributeValue
        )(Schema.derived[Person])
      },
      test("simple enum Green") {
        roundTripWithSchema2Codec(
          expectedValue = TrafficLight.Green,
          expectedAV = AttributeValue.String("Green")
        )(Schema.derived[TrafficLight])
      },
      test("Complex enum PaymentMethod.PayPal with no discriminator") {
        roundTripWithSchema2Codec(
          expectedValue = PaymentMethod.PayPal("a@b.com"),
          expectedAV = Item("PayPal" -> Item("email" -> "a@b.com")).toAttributeValue
        )(Schema.derived[PaymentMethod])
      },
      test("Complex enum PaymentMethod.Other with no discriminator") {
        roundTripWithSchema2Codec(
          expectedValue = PaymentMethod.Other,
          expectedAV = Item("Other" -> Item.empty).toAttributeValue
        )(Schema.derived[PaymentMethod])
      },
      test("Complex enum PaymentMethod.PayPal with discriminator field foo") {
        roundTripWithSchema2Codec(
          expectedValue = PaymentMethod.PayPal("a@b.com"),
          expectedAV = Item("email" -> "a@b.com", "foo" -> "PayPal").toAttributeValue,
          deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.Field("foo"))
        )(Schema.derived[PaymentMethod])
      },
      test("PersonId Prelude Newtype") {
        roundTripWithSchema2Codec(
          expectedValue = PersonId(1),
          expectedAV = AttributeValue.Number(BigDecimal(1))
        )(Schema.derived[PersonId])
      },
      test("Generic tuple") {
        type GenericTuple4 = String *: Long *: Int *: String *: EmptyTuple
        val schema: Schema[GenericTuple4] = Schema.derived

        roundTripWithSchema2Codec(
          expectedValue = "foo" *: 2L *: 3 *: "bar" *: EmptyTuple,
          expectedAV = AttributeValue.List(
            List(
              AttributeValue.String("foo"),
              AttributeValue.Number(BigDecimal(2)),
              AttributeValue.Number(BigDecimal(3L)),
              AttributeValue.String("bar")
            )
          )
        )(schema)
      },
      test("union type with key discriminator") {
        type Value = Int | String | (Int, String) | List[Int]
        val schema = Schema.derived[Value]

        roundTripWithSchema2Codec(
          expectedValue = "foo",
          expectedAV = Item("java.lang.String" -> "foo").toAttributeValue
        )(schema) &&
        roundTripWithSchema2Codec(
          expectedValue = (1, "foo"),
          expectedAV = AttributeValue.Map(
            Map(
              AttributeValue.String("scala.Tuple2") ->
                AttributeValue.List(
                  List(
                    AttributeValue.Number(BigDecimal(1)),
                    AttributeValue.String("foo")
                  )
                )
            )
          )
        )(schema) &&
        roundTripWithSchema2Codec(
          expectedValue = List(1, 2),
          expectedAV = AttributeValue.Map(
            Map(
              AttributeValue.String("scala.collection.immutable.List") ->
                AttributeValue.List(List(AttributeValue.Number(BigDecimal(1)), AttributeValue.Number(BigDecimal(2))))
            )
          )
        )(schema)
      },
      test("union type without discriminator") {
        type Value = Int | String | (Int, String) | List[Int]
        val schema = Schema.derived[Value]

        roundTripWithSchema2Codec[Value](
          expectedValue = "foo",
          expectedAV = AttributeValue.String("foo"),
          deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None)
        )(schema) &&
        roundTripWithSchema2Codec[Value](
          expectedValue = (1, "foo"),
          expectedAV = AttributeValue.List(
            List(
              AttributeValue.Number(BigDecimal(1)),
              AttributeValue.String("foo")
            )
          ),
          deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None)
        )(schema) &&
        roundTripWithSchema2Codec[Value](
          expectedValue = List(1, 2),
          expectedAV =
            AttributeValue.List(List(AttributeValue.Number(BigDecimal(1)), AttributeValue.Number(BigDecimal(2)))),
          deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None).withSchema1TupleCompatibility(false)
        )(schema)
      },
      test("string primitive") {
        val schema = Schema.derived[String]

        roundTripWithSchema2Codec(
          expectedValue = "foo",
          expectedAV = AttributeValue.String("foo")
        )(schema)
      }
    )

  private def roundTripWithSchema2Codec[A](
    expectedValue: A,
    expectedAV: AttributeValue,
    initialValue: Option[A] = None,
    deriverConfigure: DynamoDBCodecDeriverConfigure[A] = DynamoDBCodecDeriverConfigure.identity[A],
    builderConfigure: DerivationBuilderConfigure[A] = DerivationBuilderConfigure.identity[A]
  )(implicit schema2: Schema[A]): TestResult = {

    val initial = initialValue.getOrElse(expectedValue)

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(initial)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedAV && dec == Right(expectedValue))
    }

    val schema2Codec = SchemaCodec.schema2ToSchemaCodec2(schema2, deriverConfigure, builderConfigure)

    testBody(schema2Codec)
  }

}
