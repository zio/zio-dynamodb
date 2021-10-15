package zio.dynamodb.codec

import zio.{ Chunk, ZIO }
import zio.dynamodb.{ Decoder, Encoder }
import zio.random.Random
import zio.schema.Schema
import zio.test.Assertion.{ equalTo, isRight }
import zio.test.{ ZSpec, _ }

object CodecRoundTripSpec extends DefaultRunnableSpec with CodecTestFixtures {

  override def spec: ZSpec[Environment, Failure] = suite("")(mainSuite, eitherSuite, optionalSuite)

  val eitherSuite = suite("either suite")(
    testM("a primitive") {
      checkM(SchemaGen.anyEitherAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGenM(schema, gen)
      }
    },
    testM("of tuples") {
      checkM(
        for {
          left  <- SchemaGen.anyTupleAndValue
          right <- SchemaGen.anyTupleAndValue
        } yield (
          Schema.EitherSchema(left._1.asInstanceOf[Schema[(Any, Any)]], right._1.asInstanceOf[Schema[(Any, Any)]]),
          Right(right._2)
        )
      ) {
        case (schema, value) => assertEncodesThenDecodesM(schema, value)
      }
    },
    testM("of sequence") {
      checkM(
        for {
          left  <- SchemaGen.anySequenceAndValue
          right <- SchemaGen.anySequenceAndValue
        } yield (
          Schema.EitherSchema(left._1.asInstanceOf[Schema[Chunk[Any]]], right._1.asInstanceOf[Schema[Chunk[Any]]]),
          Left(left._2)
        )
      ) {
        case (schema, value) => assertEncodesThenDecodesM(schema, value)
      }
    },
    testM("of records") {
      checkM(for {
        (left, a)       <- SchemaGen.anyRecordAndValue
        primitiveSchema <- SchemaGen.anyPrimitive
      } yield (Schema.EitherSchema(left, primitiveSchema), Left(a))) {
        case (schema, value) => assertEncodesThenDecodesM(schema, value)
      }
    },
    testM("of records of records") {
      checkM(for {
        (left, _)  <- SchemaGen.anyRecordOfRecordsAndValue
        (right, b) <- SchemaGen.anyRecordOfRecordsAndValue
      } yield (Schema.EitherSchema(left, right), Right(b))) {
        case (schema, value) =>
          assertEncodesThenDecodesM(schema, value)
      }
    },
    testM("mixed") {
      checkM(for {
        (left, _)      <- SchemaGen.anyEnumerationAndValue
        (right, value) <- SchemaGen.anySequenceAndValue
      } yield (Schema.EitherSchema(left, right), Right(value))) {
        case (schema, value) => assertEncodesThenDecodesM(schema, value)
      }
    }
  )

  val optionalSuite = suite("optional suite")(
    testM("of primitive") {
      checkM(SchemaGen.anyOptionalAndValue) {
        case (schema, value) => assertEncodesThenDecodesM(schema, value)
      }
    },
    testM("of tuple") {
      checkM(SchemaGen.anyTupleAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodesM(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodesM(Schema.Optional(schema), None)
      }
    },
    testM("of record") {
      checkM(SchemaGen.anyRecordAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodesM(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodesM(Schema.Optional(schema), None)
      }
    },
    testM("of enumeration") {
      checkM(SchemaGen.anyEnumerationAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodesM(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodesM(Schema.Optional(schema), None)
      }
    },
    testM("of sequence") {
      checkM(SchemaGen.anySequenceAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodesM(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodesM(Schema.Optional(schema), None)
      }
    }
  )

  val mainSuite = suite("encode and decode round trip suite")(
    test("unit") {
      assertEncodesThenDecodes(Schema[Unit], ())
    },
    testM("a primitive") {
      checkM(SchemaGen.anyPrimitiveAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGenM(schema, gen)
      }
    },
    testM("either of primitive") {
      checkM(SchemaGen.anyEitherAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGenM(schema, gen)
      }
    },
    testM("of enumeration") {
      checkM(SchemaGen.anyEnumerationAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGenM(schema, gen)
      }
    },
    testM("optional of primitive") {
      checkM(SchemaGen.anyOptionalAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGenM(schema, gen)
      }
    },
    testM("tuple of primitive") {
      checkM(SchemaGen.anyTupleAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGenM(schema, gen)
      }
    },
    testM("sequence of primitive") {
      checkM(SchemaGen.anySequenceAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGenM(schema, gen)
      }
    }
  )

  private def assertEncodesThenDecodesWithGenM[A](schema: Schema[A], genA: Gen[Random with Sized, A]) =
    check(genA) { a =>
      assertEncodesThenDecodes(schema, a)
    }

  private def assertEncodesThenDecodes[A](schema: Schema[A], a: A) = {
    val enc = Encoder(schema)
    val dec = Decoder(schema)

    val encoded = enc(a)
    val decoded = dec(encoded)
    assert(decoded)(isRight(equalTo(a)))
  }

  private def assertEncodesThenDecodesM[A](schema: Schema[A], a: A) =
    ZIO.succeed(assertEncodesThenDecodes(schema, a))

}
