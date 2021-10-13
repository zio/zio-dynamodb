package zio.dynamodb.codec

import zio.Chunk
import zio.random.Random
import zio.schema.Schema
import zio.test.{ Gen, Sized }

import scala.collection.immutable.ListMap

object SchemaGen {

  val anyPrimitive: Gen[Random, Schema.Primitive[_]] =
    StandardTypeGen.anyStandardType.map(Schema.Primitive(_))

  type PrimitiveAndGen[A] = (Schema.Primitive[A], Gen[Random with Sized, A])

  val anyPrimitiveAndGen: Gen[Random, PrimitiveAndGen[_]] =
    StandardTypeGen.anyStandardTypeAndGen.map {
      case (standardType, gen) => Schema.Primitive(standardType) -> gen
    }

  def anyEnumeration[A](schema: Schema[A]): Gen[Random with Sized, ListMap[String, Schema[A]]] =
    Gen.listOfBounded(1, 10)(Gen.anyString.map(_ -> schema)).map(ListMap.empty ++ _)

  type EnumerationAndGen = (Schema[(String, _)], Gen[Random with Sized, (String, _)])

  val anyEnumerationAndGen: Gen[Random with Sized, EnumerationAndGen] =
    for {
      primitiveAndGen <- anyPrimitiveAndGen
      structure       <- anyEnumeration(primitiveAndGen._1)
      primitiveValue  <- primitiveAndGen._2
    } yield {
      val gen = Gen.oneOf(structure.keys.map(Gen.const(_)).toSeq: _*).map(l => l -> primitiveValue)
      Schema.enumeration(structure) -> gen
    }

  type EitherAndGen[A, B] = (Schema.EitherSchema[A, B], Gen[Random with Sized, Either[A, B]])

  val anyEitherAndGen: Gen[Random with Sized, EitherAndGen[_, _]] =
    for {
      (leftSchema, leftGen)   <- anyPrimitiveAndGen
      (rightSchema, rightGen) <- anyPrimitiveAndGen
    } yield (Schema.EitherSchema(leftSchema, rightSchema), Gen.either(leftGen, rightGen))

  type OptionalAndGen[A] = (Schema.Optional[A], Gen[Random with Sized, Option[A]])

  val anyOptionalAndGen: Gen[Random with Sized, OptionalAndGen[_]] =
    anyPrimitiveAndGen.map {
      case (schema, gen) => Schema.Optional(schema) -> Gen.option(gen)
    }

  type TupleAndGen[A, B] = (Schema.Tuple[A, B], Gen[Random with Sized, (A, B)])

  val anyTupleAndGen: Gen[Random with Sized, TupleAndGen[_, _]] =
    for {
      (schemaA, genA) <- anyPrimitiveAndGen
      (schemaB, genB) <- anyPrimitiveAndGen
    } yield Schema.Tuple(schemaA, schemaB) -> genA.zip(genB)

  type SequenceAndGen[A] = (Schema[Chunk[A]], Gen[Random with Sized, Chunk[A]])

  val anySequenceAndGen: Gen[Random with Sized, SequenceAndGen[_]] =
    anyPrimitiveAndGen.map {
      case (schema, gen) =>
        Schema.chunk(schema) -> Gen.chunkOf(gen)
    }

}
