package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Modifier, Optic, Schema }
import zio.dynamodb.{ AttributeValue, DynamoDBError, Item, SchemaCodec, ToAttributeValue }
import zio.prelude.Newtype
import zio.test.{ assertTrue, Spec, TestAspect, TestResult, ZIOSpecDefault }

object DynamoDBCodecDeriverVersionSpecificSpec extends ZIOSpecDefault {

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

  override def spec: Spec[Any, Any] =
    suite("Scala 3 codec suite")(
      suite("records")(
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
        }
      ),
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
          )(schema1) /* &&
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
          )(schema2) */
        }, // @@ TestAspect.ignore, // TODO: Avi - ignore until recursive cache in codec is implemented
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
        test("nested variants without discriminator") {
          type Value = Int | String | (Int, String) | List[Int]

          sealed trait Base derives Schema

          case class Case1(value: Value) extends Base

          case class Case2(value: Map[String, Long]) extends Base

          roundTripWithSchema2Codec[Base](
            expectedValue = Case1(1),
            expectedAV = Item("value" -> 1).toAttributeValue,
            deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None)
          ) &&
          roundTripWithSchema2Codec[Base](
            expectedValue = Case1("foo"),
            expectedAV = Item("value" -> "foo").toAttributeValue,
            deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None)
          ) &&
          roundTripWithSchema2Codec[Base](
            expectedValue = Case1((1, "foo")),
            expectedAV = AttributeValue.Map(
              Map(
                AttributeValue.String("value") -> AttributeValue.List(
                  List(
                    AttributeValue.Number(BigDecimal(1)),
                    AttributeValue.String("foo")
                  )
                )
              )
            ),
            deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None)
          ) &&
          roundTripWithSchema2Codec[Base](
            expectedValue = Case1(List(1, 2)),
            expectedAV = AttributeValue.Map(
              Map(
                AttributeValue.String("value") -> AttributeValue.List(
                  List(
                    AttributeValue.Number(BigDecimal(1)),
                    AttributeValue.Number(BigDecimal(2))
                  )
                )
              )
            ),
            deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None).withSchema1TupleCompatibility(false)
          ) &&
          roundTripWithSchema2Codec[Base](
            expectedValue = Case2(Map("foo" -> 1L)),
            expectedAV = Item("value" -> Item("foo" -> 1L)).toAttributeValue,
            deriverConfigure = _.withDiscriminatorKind(DiscriminatorKind.None)
          )
        }
      ),
      test("PersonId Prelude Newtype") {
        roundTripWithSchema2Codec(
          expectedValue = PersonId(1),
          expectedAV = AttributeValue.Number(BigDecimal(1))
        )
      },
      test("sequences") {
        implicit val schema1: Schema[IArray[Int]]    = Schema.derived
        implicit val schema2: Schema[IArray[Long]]   = Schema.derived
        implicit val schema3: Schema[IArray[String]] = Schema.derived

        roundTripWithSchema2Codec(
          expectedValue = IArray(1, 2, 3),
          expectedAV = AttributeValue.List(
            List(
              AttributeValue.Number(BigDecimal(1)),
              AttributeValue.Number(BigDecimal(2)),
              AttributeValue.Number(BigDecimal(3))
            )
          ),
          compareFn = (a: IArray[Int], b: IArray[Int]) => a.toList == b.toList
        )(schema1) &&
        roundTripWithSchema2Codec(
          expectedValue = IArray(1L, 2L, 3L),
          expectedAV = AttributeValue.List(
            List(
              AttributeValue.Number(BigDecimal(1)),
              AttributeValue.Number(BigDecimal(2)),
              AttributeValue.Number(BigDecimal(3))
            )
          ),
          compareFn = (a: IArray[Long], b: IArray[Long]) => a.toList == b.toList
        )(schema2) &&
        roundTripWithSchema2Codec(
          expectedValue = IArray("A", "B", "C"),
          expectedAV = AttributeValue.List(
            List(AttributeValue.String("A"), AttributeValue.String("B"), AttributeValue.String("C"))
          ),
          compareFn = (a: IArray[String], b: IArray[String]) => a.toList == b.toList
        )(schema3)
      }
    )

  private def roundTripWithSchema2Codec[A](
    expectedValue: A,
    expectedAV: AttributeValue,
    initialValue: Option[A] = None,
    deriverConfigure: DynamoDBCodecDeriverConfigure[A] = DynamoDBCodecDeriverConfigure.identity[A],
    builderConfigure: DerivationBuilderConfigure[A] = DerivationBuilderConfigure.identity[A],
    compareFn: (A, A) => Boolean = (a1: A, a2: A) => a1 == a2
  )(implicit schema2: Schema[A]): TestResult = {
    val initial                                = initialValue.getOrElse(expectedValue)
    val testBody: SchemaCodec[A] => TestResult = { codec =>
      val enc = codec.encoder(initial)
      val dec = codec.decoder(enc)
      assertTrue(enc == expectedAV && dec.map(value => compareFn(value, expectedValue)).getOrElse(false))
    }
    val schema2Codec                           = SchemaCodec.schema2ToSchemaCodec2(schema2, deriverConfigure, builderConfigure)
    testBody(schema2Codec)
  }

}
