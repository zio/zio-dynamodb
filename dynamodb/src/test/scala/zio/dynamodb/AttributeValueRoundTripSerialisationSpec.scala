package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError
import zio.test.Assertion.{ equalTo, isRight }
import zio.test._

object AttributeValueRoundTripSerialisationSpec extends ZIOSpecDefault {
  private val serialisationSuite = suite("AttributeValue Serialisation suite")(test("round trip serialisation") {
    check(genSerializable) { s =>
      check(s.genA) { (a: s.Element) =>
        val av: AttributeValue              = s.to.toAttributeValue(a)
        val v: Either[ItemError, s.Element] = s.from.fromAttributeValue(av)
        assert(v)(isRight(equalTo(a)))
      }
    }
  })

  override def spec = serialisationSuite

  trait Serializable  {
    def genA: Gen[Sized, Element]
    def to: ToAttributeValue[Element]
    def from: FromAttributeValue[Element]
    type Element
  }
  object Serializable {
    def apply[A](
      genA0: Gen[Sized, A],
      to0: ToAttributeValue[A],
      from0: FromAttributeValue[A]
    ): Serializable {
      type Element = A
    } =
      new Serializable {
        override def genA: Gen[Sized, Element]         = genA0
        override def to: ToAttributeValue[Element]     = to0
        override def from: FromAttributeValue[Element] = from0
        override type Element = A
      }
  }

  private val bigDecimalGen = Gen.bigDecimal(BigDecimal("0.0"), BigDecimal("10.0"))

  private val serializableBinary: Serializable =
    Serializable(Gen.listOf(Gen.byte), ToAttributeValue[Iterable[Byte]], FromAttributeValue[Iterable[Byte]])

  private val serializableBool: Serializable =
    Serializable(Gen.boolean, ToAttributeValue[Boolean], FromAttributeValue[Boolean])

  private val serializableString: Serializable =
    Serializable(Gen.string, ToAttributeValue[String], FromAttributeValue[String])

  private val anyNumberGen = Gen.oneOf(
    Gen.const(Serializable(Gen.short, ToAttributeValue[Short], FromAttributeValue[Short])),
    Gen.const(Serializable(Gen.int, ToAttributeValue[Int], FromAttributeValue[Int])),
    Gen.const(Serializable(Gen.long, ToAttributeValue[Long], FromAttributeValue[Long])),
    Gen.const(Serializable(Gen.float, ToAttributeValue[Float], FromAttributeValue[Float])),
    Gen.const(Serializable(Gen.double, ToAttributeValue[Double], FromAttributeValue[Double])),
    Gen.const(Serializable(bigDecimalGen, ToAttributeValue[BigDecimal], FromAttributeValue[BigDecimal]))
  )

  private def serializableOption[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Sized, V]
  ): Serializable =
    Serializable(
      Gen.option(genV),
      ToAttributeValue[Option[V]],
      FromAttributeValue[Option[V]]
    )

  private def serializableMap[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Sized, V]
  ): Serializable =
    Serializable(Gen.mapOf(Gen.string, genV), ToAttributeValue[Map[String, V]], FromAttributeValue[Map[String, V]])

  private def serializableList[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Sized, V]
  ): Serializable =
    Serializable(Gen.listOf(genV), ToAttributeValue[Iterable[V]], FromAttributeValue[Iterable[V]])

  private val serializableStringSet: Serializable =
    Serializable(Gen.setOf(Gen.string), ToAttributeValue[Set[String]], FromAttributeValue[Set[String]])

  private val serializableBinarySet: Serializable =
    Serializable(
      Gen.setOf(Gen.listOf(Gen.byte)),
      ToAttributeValue[Iterable[Iterable[Byte]]],
      FromAttributeValue[Iterable[Iterable[Byte]]]
    )

  private val anyNumberSetGen = Gen.oneOf(
    Gen.const(Serializable(Gen.setOf(Gen.short), ToAttributeValue[Set[Short]], FromAttributeValue[Set[Short]])),
    Gen.const(Serializable(Gen.setOf(Gen.int), ToAttributeValue[Set[Int]], FromAttributeValue[Set[Int]])),
    Gen.const(Serializable(Gen.setOf(Gen.long), ToAttributeValue[Set[Long]], FromAttributeValue[Set[Long]])),
    Gen.const(Serializable(Gen.setOf(Gen.float), ToAttributeValue[Set[Float]], FromAttributeValue[Set[Float]])),
    Gen.const(Serializable(Gen.setOf(Gen.double), ToAttributeValue[Set[Double]], FromAttributeValue[Set[Double]])),
    Gen.const(
      Serializable(Gen.setOf(bigDecimalGen), ToAttributeValue[Set[BigDecimal]], FromAttributeValue[Set[BigDecimal]])
    )
  )

  private val anyOptionGen = Gen.oneOf(
    Gen.const(serializableOption[Boolean](Gen.boolean)),
    Gen.const(serializableOption[String](Gen.string)),
    Gen.const(serializableOption[Short](Gen.short)),
    Gen.const(serializableOption[Int](Gen.int)),
    Gen.const(serializableOption[Float](Gen.float)),
    Gen.const(serializableOption[Double](Gen.double)),
    Gen.const(serializableOption[BigDecimal](bigDecimalGen))
  )

  private val anyMapGen = Gen.oneOf(
    Gen.const(serializableMap[Boolean](Gen.boolean)),
    Gen.const(serializableMap[String](Gen.string)),
    Gen.const(serializableMap[Short](Gen.short)),
    Gen.const(serializableMap[Int](Gen.int)),
    Gen.const(serializableMap[Float](Gen.float)),
    Gen.const(serializableMap[Double](Gen.double)),
    Gen.const(serializableMap[BigDecimal](bigDecimalGen))
  )

  private val anyListGen = Gen.oneOf(
    Gen.const(serializableList[Boolean](Gen.boolean)),
    Gen.const(serializableList[String](Gen.string)),
    Gen.const(serializableList[Short](Gen.short)),
    Gen.const(serializableList[Int](Gen.int)),
    Gen.const(serializableList[Float](Gen.float)),
    Gen.const(serializableList[Double](Gen.double)),
    Gen.const(serializableList[BigDecimal](bigDecimalGen))
  )

  private lazy val genSerializable: Gen[Sized, Serializable] =
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
