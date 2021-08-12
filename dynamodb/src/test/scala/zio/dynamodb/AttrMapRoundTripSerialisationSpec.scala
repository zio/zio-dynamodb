package zio.dynamodb

import zio.random.Random
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
  object Generators {
    val maxFields                                     = 5
//    val binaryGen: Gen[Any, List[Byte]]       = Gen.const(List(Byte.MinValue))
    val fieldNameGen                                  = Gen.anyString
    val binaryGen: Gen[Random with Sized, List[Byte]] = Gen.listOf(Gen.byte(Byte.MinValue, Byte.MaxValue))
    val binarySetGen                                  = Gen.setOf(binaryGen).map(AttributeValue.BinarySet)
    val bigDecimalGen                                 = Gen.bigDecimal(BigDecimal("0.0"), BigDecimal("10.0"))
    val anyNumberGen: Gen[Random, Any]                =
      Gen.oneOf(Gen.anyInt, Gen.anyDouble, Gen.anyLong, Gen.anyFloat, /* TODO Gen.anyShort ,*/ bigDecimalGen)
    val nullGen                                       = Gen.const(null)
    val stringGen                                     = Gen.anyString
    val stringSetGen                                  = Gen.setOf(stringGen)

    val binaryAttrGen: Gen[Random with Sized, AttributeValue.Binary] = binaryGen.map(AttributeValue.Binary)
    val boolAttrGen: Gen[Random, AttributeValue.Bool]                = Gen.boolean.map(AttributeValue.Bool(_))
    val numberAttrGen: Gen[Random with Sized, AttributeValue.Number] =
      anyNumberGen.map(n => AttributeValue.Number(BigDecimal(n.toString)))
    val stringAttrGen: Gen[Random with Sized, AttributeValue.String] = stringGen.map(AttributeValue.String)

    val anyScalarAttrGen: Gen[Random with Sized, AttributeValue] = Gen.oneOf(binaryAttrGen, boolAttrGen, stringAttrGen)

    def root: Gen[Random with Sized, AttrMap] = ???
  }

  final case class Serializable[A](
    genA: Gen[Random with Sized, A],
    to: ToAttributeValue[A],
    from: FromAttributeValue[A]
  )

  val serializableBool: Serializable[Boolean] =
    Serializable(Gen.boolean, ToAttributeValue[Boolean], FromAttributeValue[Boolean])

  val serializableString: Serializable[String] =
    Serializable(Gen.anyString, ToAttributeValue[String], FromAttributeValue[String])

  def serializableMap[V: ToAttributeValue: FromAttributeValue](
    genV: Gen[Random with Sized, V]
  ): Serializable[Map[String, V]] =
    Serializable(Gen.mapOf(Gen.anyString, genV), ToAttributeValue[Map[String, V]], FromAttributeValue[Map[String, V]])

  val genSerializable: Gen[Random with Sized, Serializable[_]] =
    Gen.oneOf(Gen.const(serializableBool), Gen.const(serializableString))

//  override def spec: ZSpec[Environment, Failure] = suite("AttrMapRoundTripSerialisationSpec suite")(fooSuite)

//  val fooSuite = suite("Foo suite")(testM("foo") {
//    check(Generators.anyNumberGen) { n =>
//      val av1 = Generators.attrMap("f2", List(""))
//      val av2 = Generators.attrMap("f1", n)
//      println(s"av1=${av1} av2=${av2}")
//      assertCompletes
//    }
//  })

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] = ???
}
