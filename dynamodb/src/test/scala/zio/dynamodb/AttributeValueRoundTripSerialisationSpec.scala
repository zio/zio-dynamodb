package zio.dynamodb

import zio.random.Random
import zio.test.Assertion.{ equalTo, isSome }
import zio.test.{ DefaultRunnableSpec, _ }

object AttributeValueRoundTripSerialisationSpec extends DefaultRunnableSpec {
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

  private val bigDecimalGen = Gen.bigDecimal(BigDecimal("0.0"), BigDecimal("10.0"))

  private val serializableBinary: Serializable =
    Serializable(Gen.listOf(Gen.anyByte), ToAttributeValue[Iterable[Byte]], FromAttributeValue[Iterable[Byte]])

  private val serializableBool: Serializable =
    Serializable(Gen.boolean, ToAttributeValue[Boolean], FromAttributeValue[Boolean])

  private val serializableString: Serializable =
    Serializable(Gen.anyString, ToAttributeValue[String], FromAttributeValue[String])

  private val anyNumberGen = Gen.oneOf(
    Gen.const(Serializable(Gen.anyShort, ToAttributeValue[Short], FromAttributeValue[Short])),
    Gen.const(Serializable(Gen.anyInt, ToAttributeValue[Int], FromAttributeValue[Int])),
    Gen.const(Serializable(Gen.anyLong, ToAttributeValue[Long], FromAttributeValue[Long])),
    Gen.const(Serializable(Gen.anyFloat, ToAttributeValue[Float], FromAttributeValue[Float])),
    Gen.const(Serializable(Gen.anyDouble, ToAttributeValue[Double], FromAttributeValue[Double])),
    Gen.const(Serializable(bigDecimalGen, ToAttributeValue[BigDecimal], FromAttributeValue[BigDecimal]))
  )

  private def serializableOption[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Random with Sized, V]
  ): Serializable =
    Serializable(
      Gen.option(genV),
      ToAttributeValue[Option[V]],
      FromAttributeValue[Option[V]]
    )

  private def serializableMap[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Random with Sized, V]
  ): Serializable =
    Serializable(Gen.mapOf(Gen.anyString, genV), ToAttributeValue[Map[String, V]], FromAttributeValue[Map[String, V]])

  private def serializableList[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Random with Sized, V]
  ): Serializable =
    Serializable(Gen.listOf(genV), ToAttributeValue[Iterable[V]], FromAttributeValue[Iterable[V]])

  private val serializableStringSet: Serializable =
    Serializable(Gen.setOf(Gen.anyString), ToAttributeValue[Set[String]], FromAttributeValue[Set[String]])

  private val serializableBinarySet: Serializable =
    Serializable(
      Gen.setOf(Gen.listOf(Gen.anyByte)),
      ToAttributeValue[Iterable[Iterable[Byte]]],
      FromAttributeValue[Iterable[Iterable[Byte]]]
    )

  private val anyNumberSetGen = Gen.oneOf(
    Gen.const(Serializable(Gen.setOf(Gen.anyShort), ToAttributeValue[Set[Short]], FromAttributeValue[Set[Short]])),
    Gen.const(Serializable(Gen.setOf(Gen.anyInt), ToAttributeValue[Set[Int]], FromAttributeValue[Set[Int]])),
    Gen.const(Serializable(Gen.setOf(Gen.anyLong), ToAttributeValue[Set[Long]], FromAttributeValue[Set[Long]])),
    Gen.const(Serializable(Gen.setOf(Gen.anyFloat), ToAttributeValue[Set[Float]], FromAttributeValue[Set[Float]])),
    Gen.const(Serializable(Gen.setOf(Gen.anyDouble), ToAttributeValue[Set[Double]], FromAttributeValue[Set[Double]])),
    Gen.const(
      Serializable(Gen.setOf(bigDecimalGen), ToAttributeValue[Set[BigDecimal]], FromAttributeValue[Set[BigDecimal]])
    )
  )

  private val anyOptionGen = Gen.oneOf(
    Gen.const(serializableOption[Boolean](Gen.boolean)),
    Gen.const(serializableOption[String](Gen.anyString)),
    Gen.const(serializableOption[Short](Gen.anyShort)),
    Gen.const(serializableOption[Int](Gen.anyInt)),
    Gen.const(serializableOption[Float](Gen.anyFloat)),
    Gen.const(serializableOption[Double](Gen.anyDouble)),
    Gen.const(serializableOption[BigDecimal](bigDecimalGen))
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

  private val anyListGen = Gen.oneOf(
    Gen.const(serializableList[Boolean](Gen.boolean)),
    Gen.const(serializableList[String](Gen.anyString)),
    Gen.const(serializableList[Short](Gen.anyShort)),
    Gen.const(serializableList[Int](Gen.anyInt)),
    Gen.const(serializableList[Float](Gen.anyFloat)),
    Gen.const(serializableList[Double](Gen.anyDouble)),
    Gen.const(serializableList[BigDecimal](bigDecimalGen))
  )

  private lazy val genSerializable: Gen[Random with Sized, Serializable] =
    Gen.oneOf(
      Gen.const(serializableBinary),
      Gen.const(serializableBinarySet),
      Gen.const(serializableBool),
      Gen.const(serializableString),
      Gen.const(serializableStringSet),
      anyOptionGen,
      anyNumberGen,
      anyNumberSetGen,
      anyMapGen,
      anyListGen
    )

}
