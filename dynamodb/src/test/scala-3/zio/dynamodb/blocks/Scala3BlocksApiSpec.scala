package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Modifier, Optic, Schema }
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

  override def spec =
    suite("Scala 3 codec suite")(
      test("Scala 3 enums") {
        assertTrue(1 == 1)
      },
      testRoundTripWithSchema2Codec("Person with simple enum")(Schema.derived[Person])(
        expectedItem = Item("id" -> OpaqueId(123), "name" -> "John Doe", "trafficLight" -> "Green").toAttributeValue
      )(
        expectedValue = Person(
          id = 123,
          name = "John Doe",
          trafficLight = TrafficLight.Green
        )
      ),
      testRoundTripWithSchema2Codec("simple enum Green")(Schema.derived[TrafficLight])(
        expectedItem = AttributeValue.String("Green")
      )(
        expectedValue = TrafficLight.Green
      ),
      testRoundTripWithSchema2Codec("Complex enum PaymentMethod.PayPal with no discriminator")(
        Schema.derived[PaymentMethod]
      )(
        expectedItem = Item("PayPal" -> Item("email" -> "a@b.com")).toAttributeValue
      )(
        expectedValue = PaymentMethod.PayPal("a@b.com")
      ),
      testRoundTripWithSchema2Codec("Complex enum PaymentMethod.Other with no discriminator")(
        Schema.derived[PaymentMethod]
      )(
        expectedItem = Item("Other" -> Item.empty).toAttributeValue
      )(
        expectedValue = PaymentMethod.Other
      ),
      testRoundTripWithSchema2Codec("Complex enum PaymentMethod.PayPal with discriminator field foo")(
        Schema.derived[PaymentMethod],
        _.withDiscriminatorKind(DiscriminatorKind.Field("foo"))
      )(
        expectedItem = Item("email" -> "a@b.com", "foo" -> "PayPal").toAttributeValue
      )(
        expectedValue = PaymentMethod.PayPal("a@b.com")
      ),
      testRoundTripWithSchema2Codec("PersonId Prelude Newtype")(Schema.derived[PersonId])(
        expectedItem = AttributeValue.Number(BigDecimal(1))
      )(
        expectedValue = PersonId(1)
      )
    )

  private def testRoundTripWithSchema2Codec[A](
    name: String
  )(
    schema2: Schema[A],
    deriverConfigure: DynamoDBCodecDeriverConfigure[A] = DynamoDBCodecDeriverConfigure.identity[A],
    builderConfigure: DerivationBuilderConfigure[A] = DerivationBuilderConfigure.identity[A]
  )(
    expectedItem: AttributeValue
  )(
    initialValue: Option[A] = None,
    expectedValue: A
  ): Spec[Any, Nothing] = {

    val initial = initialValue.getOrElse(expectedValue)

    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(initial)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedItem && dec == Right(expectedValue))
    }

    val schema2Codec = SchemaCodec.schema2ToSchemaCodec2(schema2, deriverConfigure, builderConfigure)

    suite(name)(
      test("schema2") {
        testBody(schema2Codec)
      }
    )
  }

}
