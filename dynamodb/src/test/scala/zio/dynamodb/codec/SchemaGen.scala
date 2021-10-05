package zio.dynamodb.codec

import zio.random.Random
import zio.schema.Schema
import zio.test.{ Gen, Sized }

object SchemaGen {
  val anyPrimitive: Gen[Random, Schema.Primitive[_]] =
    StandardTypeGen.anyStandardType.map(Schema.Primitive(_))

  type PrimitiveAndGen[A] = (Schema.Primitive[A], Gen[Random with Sized, A])

  val anyPrimitiveAndGen: Gen[Random, PrimitiveAndGen[_]] =
    StandardTypeGen.anyStandardTypeAndGen.map {
      case (standardType, gen) => Schema.Primitive(standardType) -> gen
    }

  type EitherAndGen[A, B] = (Schema.EitherSchema[A, B], Gen[Random with Sized, Either[A, B]])

  val anyEitherAndGen: Gen[Random with Sized, EitherAndGen[_, _]] =
    for {
      (leftSchema, leftGen)   <- anyPrimitiveAndGen
      (rightSchema, rightGen) <- anyPrimitiveAndGen
    } yield (Schema.EitherSchema(leftSchema, rightSchema), Gen.either(leftGen, rightGen))
}
