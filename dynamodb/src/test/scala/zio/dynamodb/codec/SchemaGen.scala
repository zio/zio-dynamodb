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
}
