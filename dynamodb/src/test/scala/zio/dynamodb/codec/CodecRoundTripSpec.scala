package zio.dynamodb.codec

import zio.{ Chunk, ZIO }
import zio.dynamodb.{ Decoder, Encoder }
//import zio.json.{ DeriveJsonEncoder, JsonEncoder }
import zio.random.Random
import zio.schema.{ DeriveSchema, Schema, StandardType }
import zio.test.Assertion.{ equalTo, isRight }
import zio.test.{ ZSpec, _ }

import scala.collection.immutable.ListMap

object CodecRoundTripSpec extends DefaultRunnableSpec with CodecTestFixtures {

  override def spec: ZSpec[Environment, Failure] =
    suite("encode then decode suite")(
      simpleSuite,
      eitherSuite,
      optionalSuite,
      caseClassSuite,
      recordSuite,
      sequenceSuite,
      enumerationSuite,
      transformSuite,
      anySchemaSuite
    )

  private val eitherSuite = suite("either suite")(
    testM("a primitive") {
      checkM(SchemaGen.anyEitherAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
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
        case (schema, value) => assertEncodesThenDecodes(schema, value)
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
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    testM("of records") {
      checkM(for {
        (left, a)       <- SchemaGen.anyRecordAndValue
        primitiveSchema <- SchemaGen.anyPrimitive
      } yield (Schema.EitherSchema(left, primitiveSchema), Left(a))) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    testM("of records of records") {
      checkM(for {
        (left, _)  <- SchemaGen.anyRecordOfRecordsAndValue
        (right, b) <- SchemaGen.anyRecordOfRecordsAndValue
      } yield (Schema.EitherSchema(left, right), Right(b))) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    },
    testM("mixed") {
      checkM(for {
        (left, _)      <- SchemaGen.anyEnumerationAndValue
        (right, value) <- SchemaGen.anySequenceAndValue
      } yield (Schema.EitherSchema(left, right), Right(value))) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    }
  )

  private val optionalSuite = suite("optional suite")(
    testM("of primitive") {
      checkM(SchemaGen.anyOptionalAndValue) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    testM("of tuple") {
      checkM(SchemaGen.anyTupleAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema), None)
      }
    },
    testM("of record") {
      checkM(SchemaGen.anyRecordAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema), None)
      }
    },
    testM("of enumeration") {
      checkM(SchemaGen.anyEnumerationAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema), None)
      }
    },
    testM("of sequence") {
      checkM(SchemaGen.anySequenceAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema), None)
      }
    }
  )

  private val sequenceSuite = suite("sequence")(
    testM("of primitives") {
      checkM(SchemaGen.anySequenceAndValue) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    testM("of records") {
      checkM(SchemaGen.anyCaseClassAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.chunk(schema), Chunk.fill(3)(value))
      }
    },
    testM("of java.time.ZoneOffset") {
      //FIXME test independently because including ZoneOffset in StandardTypeGen.anyStandardType wreaks havoc.
      checkM(Gen.chunkOf(JavaTimeGen.anyZoneOffset)) { chunk =>
        assertEncodesThenDecodes(
          Schema.chunk(Schema.Primitive(StandardType.ZoneOffset)),
          chunk
        )
      }
    }
  )

  private val caseClassSuite = suite("case class")(
//    testM("basic") {
//      checkM(searchRequestGen) { value =>
//        assertEncodesThenDecodes(searchRequestSchema, value)
//      }
//    },
    testM("object") {
      assertEncodesThenDecodes(schemaObject, Singleton)
    }
  )

  private val recordSuite = suite("record")(
    testM("any") {
      checkM(SchemaGen.anyRecordAndValue) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    testM("minimal test case") {
      SchemaGen.anyRecordAndValue.runHead.flatMap {
        case Some((schema, value)) =>
          val key      = new String(Array('\u0007', '\n'))
          val embedded = Schema.record(Schema.Field(key, schema))
          assertEncodesThenDecodes(embedded, ListMap(key -> value))
        case None                  => ZIO.fail("Should never happen!")
      }
    },
    testM("record of records") {
      checkM(SchemaGen.anyRecordOfRecordsAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    },
    testM("of primitives") {
      checkM(SchemaGen.anyRecordAndValue) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    testM("of ZoneOffsets") {
      checkM(JavaTimeGen.anyZoneOffset) { zoneOffset =>
        assertEncodesThenDecodes(
          Schema.record(Schema.Field("zoneOffset", Schema.Primitive(StandardType.ZoneOffset))),
          ListMap[String, Any]("zoneOffset" -> zoneOffset)
        )
      }
    },
    testM("of record") {
      assertEncodesThenDecodes(
        nestedRecordSchema,
        ListMap[String, Any]("l1" -> "s", "l2" -> ListMap[String, Any]("foo" -> "s", "bar" -> 1))
      )
    }
  )

  private val enumerationSuite = suite("enumeration")(
    testM("of primitives") {
      assertEncodesThenDecodes(
        enumSchema,
        "foo"
      )
    },
    testM("ADT") {
      assertEncodesThenDecodes(
        Schema[Enumeration],
        Enumeration(StringValue("foo"))
      ) &> assertEncodesThenDecodes(Schema[Enumeration], Enumeration(IntValue(-1))) &> assertEncodesThenDecodes(
        Schema[Enumeration],
        Enumeration(BooleanValue(false))
      )
    }
  )

  private val transformSuite = suite("transform")(
    testM("any") {
      checkM(SchemaGen.anyTransformAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    }
  )

  private val simpleSuite = suite("simple suite")(
    test("unit") {
      assertEncodesThenDecodesPure(Schema[Unit], ())
    },
    testM("a primitive") {
      checkM(SchemaGen.anyPrimitiveAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    },
    testM("either of primitive") {
      checkM(SchemaGen.anyEitherAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    },
    testM("of enumeration") {
      checkM(SchemaGen.anyEnumerationAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    },
    testM("optional of primitive") {
      checkM(SchemaGen.anyOptionalAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    },
    testM("tuple of primitive") {
      checkM(SchemaGen.anyTupleAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    },
    testM("sequence of primitive") {
      checkM(SchemaGen.anySequenceAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    }
  )

  private val anySchemaSuite = suite("any schema")(
    testM("leaf") {
      checkM(SchemaGen.anyLeafAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    },
    testM("recursive schema") {
      checkM(SchemaGen.anyTreeAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    },
    testM("recursive data type") {
      checkM(SchemaGen.anyRecursiveTypeAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    }
  )

  private def assertEncodesThenDecodesWithGen[A](schema: Schema[A], genA: Gen[Random with Sized, A]) =
    check(genA) { a =>
      assertEncodesThenDecodesPure(schema, a)
    }

  private def assertEncodesThenDecodesPure[A](schema: Schema[A], a: A) = {
    val enc = Encoder(schema)
    val dec = Decoder(schema)

    val encoded = enc(a)
    val decoded = dec(encoded)
    assert(decoded)(isRight(equalTo(a)))
  }

  private def assertEncodesThenDecodes[A](schema: Schema[A], a: A) =
    ZIO.succeed(assertEncodesThenDecodesPure(schema, a))

  case class SearchRequest(query: String, pageNumber: Int, resultPerPage: Int)

//  object SearchRequest {
//    implicit val encoder: JsonEncoder[SearchRequest] = DeriveJsonEncoder.gen[SearchRequest]
//  }
//
//  val searchRequestGen: Gen[Random with Sized, SearchRequest] =
//    for {
//      query      <- Gen.anyString
//      pageNumber <- Gen.int(Int.MinValue, Int.MaxValue)
//      results    <- Gen.int(Int.MinValue, Int.MaxValue)
//    } yield SearchRequest(query, pageNumber, results)
//
//  val searchRequestSchema: Schema[SearchRequest] = DeriveSchema.gen[SearchRequest]

  sealed trait OneOf
  case class StringValue(value: String)   extends OneOf
  case class IntValue(value: Int)         extends OneOf
  case class BooleanValue(value: Boolean) extends OneOf

  object OneOf {
    implicit val schema: Schema[OneOf] = DeriveSchema.gen[OneOf]
  }

  case class Enumeration(oneOf: OneOf)

  object Enumeration {
    implicit val schema: Schema[Enumeration] = DeriveSchema.gen[Enumeration]
  }

  case object Singleton
  implicit val schemaObject: Schema[Singleton.type] = DeriveSchema.gen[Singleton.type]

  val nestedRecordSchema: Schema[ListMap[String, _]] = Schema.record(
    Schema.Field("l1", Schema.Primitive(StandardType.StringType)),
    Schema.Field("l2", recordSchema)
  )
}
