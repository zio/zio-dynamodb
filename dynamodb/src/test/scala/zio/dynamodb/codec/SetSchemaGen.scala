package zio.dynamodb.codec

import zio.Chunk
import zio.dynamodb.codec.SchemaGen.anyPrimitiveAndGen
import zio.dynamodb.{ AttributeValue, Codec }
import zio.schema.{ Schema, StandardType }
import zio.test.Assertion.{ equalTo, isRight }
import zio.test.{ assert, assertCompletes, Gen, Sized }

object SetSchemaGen {

  sealed trait SetType
  object SetType {
    case object None      extends SetType
    case object StringSet extends SetType
    case object NumberSet extends SetType
    case object BinarySet extends SetType
  }

  def setType[A](standardType: StandardType[A]): SetType =
    standardType match {
      case StandardType.StringType     => SetType.StringSet
      case StandardType.ShortType      => SetType.NumberSet
      case StandardType.IntType        => SetType.NumberSet
      case StandardType.LongType       => SetType.NumberSet
      case StandardType.FloatType      => SetType.NumberSet
      case StandardType.DoubleType     => SetType.NumberSet
      case StandardType.BinaryType     => SetType.BinarySet
      case StandardType.BigDecimalType => SetType.NumberSet
      case StandardType.BigIntegerType => SetType.NumberSet
      case _                           => SetType.None
    }

  type PrimitiveAndGenWithSetType[A] = (Schema.Primitive[A], Gen[Sized, A], SetType)

  def primitiveAndGenWithSetType[A]: Gen[Any, PrimitiveAndGenWithSetType[A]] =
    anyPrimitiveAndGen[A].collect[SetSchemaGen.PrimitiveAndGenWithSetType[A]] { case (s: Schema.Primitive[A], gen) =>
      (s, gen, setType(s.standardType))
    }

  type SetAndGenWithSetType[A] = (Schema.Set[A], Gen[Sized, Set[A]], SetType)

  def anySetAndGenWithSetType[A]: Gen[Sized, SetAndGenWithSetType[A]] =
    primitiveAndGenWithSetType[A].map { case (schema, gen, setType) =>
      (
        Schema.Set(schema, Chunk.empty),
        Gen.setOf(gen),
        setType
      )
    }

  type SetAndValueWithSetType[A] = (Schema.Set[A], Set[A], SetType)

  def anySetAndValueWithSetType[A]: Gen[Sized, SetAndValueWithSetType[A]] =
    for {
      (schema, gen, setType) <- anySetAndGenWithSetType[A]
      value                  <- gen
    } yield (schema, value, setType)

  def assertEncodesThenDecodesSet[A](schema: Schema[A], a: A, setType: SetType) = {
    val enc     = Codec.encoder(schema)
    val dec     = Codec.decoder(schema)
    val encoded = enc(a)
    val decoded = dec(encoded)

    val assertRoundTrip = assert(decoded)(isRight(equalTo(a)))

    val assertEncodedSetTypeIsNativeWhenPossible = (setType, encoded) match {
      case (SetType.StringSet, AttributeValue.StringSet(_)) => assertCompletes
      case (SetType.NumberSet, AttributeValue.NumberSet(_)) => assertCompletes
      case (SetType.BinarySet, AttributeValue.BinarySet(_)) => assertCompletes
      case (SetType.None, AttributeValue.List(_))           => assertCompletes
      case _                                                => !assertCompletes
    }
    assertRoundTrip && assertEncodedSetTypeIsNativeWhenPossible
  }
}
