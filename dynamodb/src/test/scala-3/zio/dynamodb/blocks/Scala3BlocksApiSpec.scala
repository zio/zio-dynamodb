package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Modifier, Optic, Schema }
import zio.dynamodb.{ AttributeValue, Item, SchemaCodec, ToAttributeValue }
import zio.prelude.Newtype
import zio.test.{ assertTrue, Spec, TestAspect, TestResult, ZIOSpecDefault }

object Scala3BlocksApiSpec extends ZIOSpecDefault {

  enum TrafficLight derives Schema {
    case Red, Yellow, Green
  }

  enum PaymentMethod derives Schema {
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
  ) derives Schema

  sealed trait Foo derives Schema

  case object Foo1 extends Foo

  sealed trait Bar extends Foo

  case object Bar1 extends Bar

  enum LinkedList[+T] {
    @Modifier.rename("::")
    case Node(
      @Modifier.rename("val") value: T,
      @Modifier.rename("nxt") next: LinkedList[T]
    )

    case End
  }

  // Add the roundTripWithSchema2Codec definition here for test usage
  private def roundTripWithSchema2Codec[A](
    expectedValue: A,
    expectedAV: AttributeValue,
    initialValue: Option[A] = None,
    deriverConfigure: DynamoDBCodecDeriverConfigure[A] = DynamoDBCodecDeriverConfigure.identity[A],
    builderConfigure: DerivationBuilderConfigure[A] = DerivationBuilderConfigure.identity[A]
  )(implicit schema2: Schema[A]): TestResult = {
    val initial                                = initialValue.getOrElse(expectedValue)
    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(initial)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedAV && dec == Right(expectedValue))
    }
    val schema2Codec                           = SchemaCodec.schema2ToSchemaCodec2(schema2, deriverConfigure, builderConfigure)
    testBody(schema2Codec)
  }

  override def spec: Spec[Any, Any] =
    suite("Scala 3 codec suite")(
      suite("variants")(
        test("constant values on different hierarchy levels") {
          roundTripWithSchema2Codec[Foo](Foo1, AttributeValue.String("Foo1")) &&
          roundTripWithSchema2Codec[Foo](Bar1, AttributeValue.String("Bar1"))
        },
        test("constant values") {
          roundTripWithSchema2Codec[TrafficLight](
            expectedValue = TrafficLight.Green,
            expectedAV = AttributeValue.String("Green")
          ) &&
          roundTripWithSchema2Codec[TrafficLight](
            expectedValue = TrafficLight.Yellow,
            expectedAV = AttributeValue.String("Yellow")
          ) &&
          roundTripWithSchema2Codec[TrafficLight](
            expectedValue = TrafficLight.Red,
            expectedAV = AttributeValue.String("Red")
          )
        },
        test("complex recursive values") {
          import LinkedList._

          val schema1 = Schema.derived[LinkedList[Int]]
          val schema2 = Schema.derived[LinkedList[Option[String]]]
          roundTripWithSchema2Codec(
            expectedValue = Node(1, Node(2, End)),
            expectedAV = Item(
              "::" -> Item("val" -> 1, "nxt" -> Item("::" -> Item("val" -> 2, "nxt" -> Item("End" -> Item.empty))))
            ).toAttributeValue
          )(schema1) &&
          roundTripWithSchema2Codec(
            expectedValue = Node(Some("VVV"), Node(None, End)),
            expectedAV = AttributeValue.Map(
              Map(
                AttributeValue.String("::") -> AttributeValue.Map(
                  Map(
                    AttributeValue.String("val") -> AttributeValue.String("VVV"),
                    AttributeValue.String("nxt") -> AttributeValue.Map(
                      Map(
                        AttributeValue.String("::") -> AttributeValue.Map(
                          Map(
                            AttributeValue.String("val") -> AttributeValue.Null,
                            AttributeValue.String("nxt") -> AttributeValue.Map(
                              Map(
                                AttributeValue.String("End") -> AttributeValue.Map(
                                  Map.empty[AttributeValue.String, AttributeValue]
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )(schema2)
        } @@ TestAspect.ignore // TODO: Avi - implement recursive cache in codec
      ),
      test("Person with simple enum") {
        roundTripWithSchema2Codec(
          expectedValue = Person(
            id = 123,
            name = "John Doe",
            trafficLight = TrafficLight.Green
          ),
          expectedAV = Item("id" -> OpaqueId(123), "name" -> "John Doe", "trafficLight" -> "Green").toAttributeValue
        )
      },
      test("simple enum Green") {
        roundTripWithSchema2Codec(
          expectedValue = TrafficLight.Green,
          expectedAV = AttributeValue.String("Green")
        ) &&
        roundTripWithSchema2Codec(
          expectedValue = TrafficLight.Yellow,
          expectedAV = AttributeValue.String("Yellow")
        ) &&
        roundTripWithSchema2Codec(
          expectedValue = TrafficLight.Red,
          expectedAV = AttributeValue.String("Red")
        )
      },
      test("Complex enum PaymentMethod.PayPal with no discriminator") {
        roundTripWithSchema2Codec(
          expectedValue = PaymentMethod.PayPal("a@b.com"),
          expectedAV = Item("PayPal" -> Item("email" -> "a@b.com")).toAttributeValue
        )
      },
      test("Complex enum PaymentMethod.Other with no discriminator") {
        roundTripWithSchema2Codec(
          expectedValue = PaymentMethod.Other,
          expectedAV = Item("Other" -> Item.empty).toAttributeValue
        )
      },
      test("Complex enum PaymentMethod.PayPal with discriminator field foo") {
        roundTripWithSchema2Codec(
          expectedValue = PaymentMethod.PayPal("a@b.com"),
          expectedAV = Item("email" -> "a@b.com", "foo" -> "PayPal").toAttributeValue,
          deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.Field("foo"))
        )
      },
      test("PersonId Prelude Newtype") {
        roundTripWithSchema2Codec(
          expectedValue = PersonId(1),
          expectedAV = AttributeValue.Number(BigDecimal(1))
        )
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
      }
    )
}
