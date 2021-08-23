package zio.dynamodb

import zio.random.Random
import zio.test.Assertion.{ equalTo, isSome }
import zio.test.{ DefaultRunnableSpec, _ }

/*
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

object AttributeValueRoundTripSerialisationSpec extends DefaultRunnableSpec {
  private val bigDecimalGen = Gen.bigDecimal(BigDecimal("0.0"), BigDecimal("10.0"))

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

  private val serializableShort: Serializable      =
    Serializable(Gen.anyShort, ToAttributeValue[Short], FromAttributeValue[Short])
  private val serializableInt: Serializable        =
    Serializable(Gen.anyInt, ToAttributeValue[Int], FromAttributeValue[Int])
  private val serializableFloat: Serializable      =
    Serializable(Gen.anyFloat, ToAttributeValue[Float], FromAttributeValue[Float])
  private val serializableDouble: Serializable     =
    Serializable(Gen.anyDouble, ToAttributeValue[Double], FromAttributeValue[Double])
  private val serializableBigDecimal: Serializable =
    Serializable(bigDecimalGen, ToAttributeValue[BigDecimal], FromAttributeValue[BigDecimal])

  private def serializableMap[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Random with Sized, V]
  ): Serializable =
    Serializable(Gen.mapOf(Gen.anyString, genV), ToAttributeValue[Map[String, V]], FromAttributeValue[Map[String, V]])

  private val serializableStringSet: Serializable =
    Serializable(Gen.setOf(Gen.anyString), ToAttributeValue[Set[String]], FromAttributeValue[Set[String]])

  private val anyNumberSetGen = Gen.oneOf(
    Gen.const(Serializable(Gen.setOf(Gen.anyShort), ToAttributeValue[Set[Short]], FromAttributeValue[Set[Short]])),
    Gen.const(Serializable(Gen.setOf(Gen.anyInt), ToAttributeValue[Set[Int]], FromAttributeValue[Set[Int]])),
    Gen.const(Serializable(Gen.setOf(Gen.anyFloat), ToAttributeValue[Set[Float]], FromAttributeValue[Set[Float]])),
    Gen.const(Serializable(Gen.setOf(Gen.anyDouble), ToAttributeValue[Set[Double]], FromAttributeValue[Set[Double]])),
    Gen.const(
      Serializable(Gen.setOf(bigDecimalGen), ToAttributeValue[Set[BigDecimal]], FromAttributeValue[Set[BigDecimal]])
    )
  )

  private val anyMapGen = Gen.oneOf(
    Gen.const(serializableMap[Boolean](Gen.boolean)),
    Gen.const(serializableMap[String](Gen.anyString)),
    Gen.const(serializableMap[Short](Gen.anyShort)),
    Gen.const(serializableMap[Int](Gen.anyInt)),
    Gen.const(serializableMap[Float](Gen.anyFloat)),
    Gen.const(serializableMap[Double](Gen.anyDouble)),
    Gen.const(serializableMap[BigDecimal](bigDecimalGen))
  )

  private val genSerializable: Gen[Random with Sized, Serializable] =
    Gen.oneOf(
      Gen.const(serializableBool),
      Gen.const(serializableString),
      Gen.const(serializableInt),
      Gen.const(serializableShort),
      Gen.const(serializableFloat),
      Gen.const(serializableDouble),
      Gen.const(serializableBigDecimal),
      anyNumberSetGen,
      anyMapGen,
      Gen.const(serializableStringSet)
    )

  private val serialisationSuite = suite("AttributeValue Serialisation suite")(testM("round trip serialisation") {
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
