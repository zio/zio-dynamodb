package zio.dynamodb

import zio.random.Random
import zio.test.Assertion.{ equalTo, isSome }
import zio.test.{ DefaultRunnableSpec, _ }

/*
AttrMap("f1", "")
AttrMap("f1", 1)

  private[dynamodb] final case class Binary(value: Iterable[Byte])                extends AttributeValue
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]])   extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                         extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])        extends AttributeValue
  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  private[dynamodb] final case class Number(value: BigDecimal)                    extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  private[dynamodb] case object Null                                              extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)                   extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString])           extends AttributeValue
 */

object AttrMapRoundTripSerialisationSpec extends DefaultRunnableSpec {

  trait Serializable  {
    def genA: Gen[Random with Sized, Element]
    def to: ToAttributeValue[Element]
    def from: FromAttributeValue[Element]
    type Element
  }
  object Serializable {
    def apply[A](
      genA0: Gen[Random with Sized, A],
      to0: ToAttributeValue[A],
      from0: FromAttributeValue[A]
    ): Serializable {
      type Element = A
    } =
      new Serializable {
        override def genA: Gen[Random with Sized, Element] = genA0
        override def to: ToAttributeValue[Element]         = to0
        override def from: FromAttributeValue[Element]     = from0
        override type Element = A
      }
  }

  private val serializableBool: Serializable =
    Serializable(Gen.boolean, ToAttributeValue[Boolean], FromAttributeValue[Boolean])

  private val serializableString: Serializable =
    Serializable(Gen.anyString, ToAttributeValue[String], FromAttributeValue[String])

  // do for other container types like List, NumberSet etc etc
  private def serializableMap[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Random with Sized, V]
  ): Serializable =
    Serializable(Gen.mapOf(Gen.anyString, genV), ToAttributeValue[Map[String, V]], FromAttributeValue[Map[String, V]])

  private val genSerializable: Gen[Random with Sized, Serializable] =
    Gen.oneOf(
      Gen.const(serializableBool),
      Gen.const(serializableString),
      Gen.const(serializableMap[Int](Gen.anyInt))
    )

  private val serialisationSuite = suite("Serialisation suite")(testM("round trip serialisation") {
    checkM(genSerializable) { s =>
      check(s.genA) { (a: s.Element) =>
        val av: AttributeValue   = s.to.toAttributeValue(a)
        val v: Option[s.Element] = s.from.fromAttributeValue(av)
        assert(v)(isSome(equalTo(a)))
      }
    }
  })

  override def spec: ZSpec[Environment, Failure] = serialisationSuite
}
