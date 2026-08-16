package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, Codec, DynamoDBQuery }
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.schema.{ DeriveSchema, Schema, StandardType }
import zio.test.Assertion.{ equalTo, isLeft, isRight }
import zio.test._
import zio.{ Chunk, ZIO }

import scala.collection.immutable.ListMap
import zio.test.{ Gen, Sized, ZIOSpecDefault }
import zio.schema.TypeId

import java.time.ZoneOffset

object CodecRoundTripSpec extends ZIOSpecDefault with CodecTestFixtures {

  override def spec: Spec[Environment, Any] =
    suite("encode then decode suite")(
      simpleSuite,
      eitherSuite,
      optionalSuite,
      caseClassSuite,
      recordSuite,
      sequenceSuite,
      enumerationSuite,
      transformSuite,
      anySchemaSuite,
      bigSchemaSuite,
      genericRecordMissingFieldSuite
    )

  private val eitherSuite = suite("either suite")(
    test("a primitive") {
      check(SchemaGen.anyEitherAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema.asInstanceOf[Schema.Either[Any, Any]], gen)
      }
    },
    test("of tuples") {
      check(
        for {
          left  <- SchemaGen.anyTupleAndValue[Any, Any]
          right <- SchemaGen.anyTupleAndValue[Any, Any]
        } yield (
          Schema.Either(left._1.asInstanceOf[Schema[(Any, Any)]], right._1.asInstanceOf[Schema[(Any, Any)]]),
          Right(right._2)
        )
      ) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    test("of sequence") {
      check(
        for {
          left  <- SchemaGen.anySequenceAndValue[Any]
          right <- SchemaGen.anySequenceAndValue[Any]
        } yield (
          Schema.Either(left._1.asInstanceOf[Schema[Chunk[Any]]], right._1.asInstanceOf[Schema[Chunk[Any]]]),
          Left(left._2)
        )
      ) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    test("of records") {
      check(for {
        (left, a)       <- SchemaGen.anyRecordAndValue()
        primitiveSchema <- SchemaGen.anyPrimitive[Any]
      } yield (Schema.Either(left, primitiveSchema), Left(a))) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    test("of records of records") {
      check(for {
        (left, _)  <- SchemaGen.anyRecordOfRecordsAndValue
        (right, b) <- SchemaGen.anyRecordOfRecordsAndValue
      } yield (Schema.Either(left, right), Right(b))) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    },
    test("mixed") {
      check(for {
        (left, _)      <- SchemaGen.anyEnumerationAndValue
        (right, value) <- SchemaGen.anySequenceAndValue[Any]
      } yield (Schema.Either(left, right), Right(value))) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    }
  )

  private val optionalSuite = suite("optional suite")(
    test("of primitive") {
      check(SchemaGen.anyOptionalAndValue) {
        case (schema, value) => assertEncodesThenDecodes(schema.asInstanceOf[Schema.Optional[Any]], value)
      }
    },
    test("of tuple") {
      check(SchemaGen.anyTupleAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema.asInstanceOf[Schema.Tuple2[Any, Any]]), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema.asInstanceOf[Schema.Tuple2[Any, Any]]), None)
      }
    },
    test("of record") {
      check(SchemaGen.anyRecordAndValue()) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema), None)
      }
    },
    test("of enumeration") {
      check(SchemaGen.anyEnumerationAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema), None)
      }
    },
    test("of sequence") {
      check(SchemaGen.anySequenceAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.Optional(schema), Some(value)) &>
            assertEncodesThenDecodes(Schema.Optional(schema), None)
      }
    }
  )

  private val sequenceSuite = suite("sequence")(
    test("of primitives") {
      check(SchemaGen.anySequenceAndValue) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    test("of records") {
      check(SchemaGen.anyCaseClassAndValue[Any]) {
        case (schema, value) =>
          assertEncodesThenDecodes(Schema.chunk(schema.asInstanceOf[Schema[Any]]), Chunk.fill(3)(value))
      }
    },
    test("of java.time.ZoneOffset") {
      //FIXME test independently because including ZoneOffset in StandardTypeGen.anyStandardType wreaks havoc.
      check(Gen.chunkOf(JavaTimeGen.anyZoneOffset)) { chunk =>
        assertEncodesThenDecodes(
          Schema.chunk(Schema.Primitive(StandardType.ZoneOffsetType)),
          chunk
        )
      }
    }
  )

  private val caseClassSuite = suite("case class")(
    test("basic") {
      check(searchRequestGen) { value =>
        assertEncodesThenDecodes(searchRequestSchema, value)
      }
    },
    test("object") {
      assertEncodesThenDecodes(schemaObject, Singleton)
    }
  )

  private val recordSuite = suite("record")(
    test("any") {
      check(SchemaGen.anyRecordAndValue()) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    test("minimal test case") {
      SchemaGen.anyRecordAndValue().runHead.flatMap {
        case Some((schema, value)) =>
          val key      = new String(Array('\u0007', '\n'))
          val embedded = Schema.record(
            TypeId.Structural,
            Schema
              .Field[ListMap[String, _], ListMap[String, _]](
                key,
                schema,
                get0 = (p: ListMap[String, _]) => p(key).asInstanceOf[ListMap[String, _]],
                set0 = (p: ListMap[String, _], v: ListMap[String, _]) => p.updated(key, v)
              )
          )
          assertEncodesThenDecodes(embedded, ListMap(key -> value))
        case None                  => ZIO.fail("Should never happen!")
      }
    },
    test("record of records") {
      check(SchemaGen.anyRecordOfRecordsAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema, value)
      }
    },
    test("of primitives") {
      check(SchemaGen.anyRecordAndValue()) {
        case (schema, value) => assertEncodesThenDecodes(schema, value)
      }
    },
    test("of ZoneOffsets") {
      check(JavaTimeGen.anyZoneOffset) { zoneOffset =>
        assertEncodesThenDecodes(
          Schema.record(
            TypeId.parse("java.time.ZoneOffset"),
            Schema.Field(
              "zoneOffset",
              Schema.Primitive(StandardType.ZoneOffsetType),
              get0 = (p: ListMap[String, _]) => p("zoneOffset").asInstanceOf[ZoneOffset],
              set0 = (p: ListMap[String, _], v: ZoneOffset) => p.updated("zoneOffset", v)
            )
          ),
          ListMap[String, Any]("zoneOffset" -> zoneOffset)
        )
      }
    },
    test("of record") {
      assertEncodesThenDecodes(
        nestedRecordSchema,
        ListMap[String, Any]("l1" -> "s", "l2" -> ListMap[String, Any]("foo" -> "s", "bar" -> 1))
      )
    }
  )

  private val enumerationSuite = suite("enumeration")(
    test("of primitives") {
      assertEncodesThenDecodes(
        enumSchema,
        "foo"
      )
    },
    test("ADT") {
      assertEncodesThenDecodes(
        Schema[Enumeration],
        Enumeration(StringValue("foo"))
      ) &> assertEncodesThenDecodes(Schema[Enumeration], Enumeration(IntValue(-1))) &> assertEncodesThenDecodes(
        Schema[Enumeration],
        Enumeration(BooleanValue(false))
      )
    },
    test("ADT with annotation") {
      assertEncodesThenDecodes(
        Schema[Enumeration2],
        Enumeration2(StringValue2("foo"))
      ) &> assertEncodesThenDecodes(
        Schema[Enumeration2],
        Enumeration2(StringValue2Multi("foo", "bar"))
      ) &> assertEncodesThenDecodes(Schema[Enumeration2], Enumeration2(IntValue2(-1))) &> assertEncodesThenDecodes(
        Schema[Enumeration2],
        Enumeration2(BooleanValue2(false))
      )
    }
  )

  private val transformSuite = suite("transform")(
    test("any") {
      check(SchemaGen.anyTransformAndValue[Any]) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema.asInstanceOf[Schema.Transform[Any, Any, String]], value)
      }
    }
  )

  private val simpleSuite = suite("simple suite")(
    test("unit") {
      assertEncodesThenDecodesPure(Schema[Unit], ())
    },
    test("a primitive") {
      check(SchemaGen.anyPrimitiveAndGen[Any]) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema.asInstanceOf[Schema.Primitive[Any]], gen)
      }
    },
    test("either of primitive") {
      check(SchemaGen.anyEitherAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema.asInstanceOf[Schema.Either[Any, Any]], gen)
      }
    },
    test("of enumeration") {
      check(SchemaGen.anyEnumerationAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    },
    test("optional of primitive") {
      check(SchemaGen.anyOptionalAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema.asInstanceOf[Schema.Optional[Any]], gen)
      }
    },
    test("tuple of primitive") {
      check(SchemaGen.anyTupleAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema.asInstanceOf[Schema.Tuple2[Any, Any]], gen)
      }
    },
    test("sequence of primitive") {
      check(SchemaGen.anySequenceAndGen) {
        case (schema, gen) =>
          assertEncodesThenDecodesWithGen(schema, gen)
      }
    },
    test("Map of string to primitive value") {
      check(SchemaGen.anyPrimitiveAndGen[Any]) {
        case (s, gen) =>
          val mapSchema = Schema.map(Schema[String], s.asInstanceOf[Schema[Any]])
          val enc       = Codec.encoder(mapSchema)
          val dec       = Codec.decoder(mapSchema)

          check(gen) { a =>
            val initialMap = Map("StringKey" -> a)
            val encoded    = enc(initialMap)
            val decoded    = dec(encoded)
            assert(decoded)(isRight(equalTo(initialMap)))
          }
      }
    },
    test("any Map") {
      check(SchemaGen.anyMapAndValue) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema.asInstanceOf[Schema.Map[Any, Any]], value.asInstanceOf[Map[Any, Any]])
      }
    },
    test("any Set") {
      import SetSchemaGen._

      check(anySetAndValueWithSetType) {
        case (schema, value, setType) =>
          assertEncodesThenDecodesSet(schema.asInstanceOf[Schema.Set[Any]], value.asInstanceOf[Set[Any]], setType)
      }
    }
  )

  private val anySchemaSuite = suite("any schema")(
    test("leaf") {
      check(SchemaGen.anyLeafAndValue[Any]) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema.asInstanceOf[Schema[Any]], value)
      }
    },
    test("recursive schema") {
      check(SchemaGen.anyTreeAndValue[Any]) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema.asInstanceOf[Schema[Any]], value)
      }
    },
    test("recursive data type") {
      check(SchemaGen.anyRecursiveTypeAndValue[Any]) {
        case (schema, value) =>
          assertEncodesThenDecodes(schema.asInstanceOf[Schema[Any]], value)
      }
    }
  )

  private val bigSchemaSuite = suite("big schema")(
    test("encodes CaseClass TwentyOne, decodes to Big") {
      // format: off
      val case21 = Case21(
        "id", "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
        "f11", "f12", "f13", "f14", "f15", "f16", "f17", None, Some("f19")
      )

      val big = Big(
        "id", "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
        "f11", "f12", "f13", "f14", "f15", "f16", "f17", None, Some("f19"),
        None, None, None, None, None, None, None, None, None, None, None
      )
      // format: on

      val item       = DynamoDBQuery.toItem(case21)
      val bigDecoded = DynamoDBQuery.fromItem[Big](item)

      assert(bigDecoded)(isRight(equalTo(big)))
    },
    test("encodes Big, decodes to CaseClass TwentyOne, preserving document level backwards compatibility") {
      // format: off
      val case21 = Case21(
        "id", "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
        "f11", "f12", "f13", "f14", "f15", "f16", "f17", None, Some("f19")
      )

      val big = Big(
        "id", "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
        "f11", "f12", "f13", "f14", "f15", "f16", "f17", None, Some("f19"),
        None, None, None, None, None, None, None, None, None, None, None
      )
      // format: on

      val itemBig       = DynamoDBQuery.toItem(big)
      val itemCase21    = DynamoDBQuery.toItem(case21)
      val case21Decoded = DynamoDBQuery.fromItem[Case21](itemBig)

      assert(case21Decoded)(isRight(equalTo(case21))) &&
      assertTrue(
        itemBig.map.values.filter(_ == AttributeValue.Null) ==
          itemCase21.map.values.filter(_ == AttributeValue.Null)
      ) &&
      assertTrue(
        !itemBig.map.values.exists(_ == AttributeValue.Null) &&
          !itemCase21.map.values.exists(_ == AttributeValue.Null)
      )
    }
  )

  // Follow-up to PR #712 (github.com/zio/zio-dynamodb/pull/712): genericRecordDecoder now
  // mirrors decodeFields's full ContainerField match for a missing field, instead of only
  // special-casing Optional. One fixture with one field per ContainerField tier
  // (genericRecordAllContainerFieldsSchema, from CodecTestFixtures) exercises
  // genericRecordDecoder directly; the Big/BigList pair separately proves the fix also
  // applies to the more mainstream, easy-to-miss trigger: any ordinary case class with more
  // than 22 fields, where zio-schema's own derivation macro silently falls back to
  // Schema.GenericRecord (CaseClassN stops at 22).
  private val genericRecordMissingFieldSuite = {
    val scalarField   = toAvString("scalarField")   -> toAvString("hello")
    val optionalField = toAvString("optionalField") -> toAvString("present")
    val chunkField    = toAvString("chunkField")    -> AttributeValue.List(Chunk(toAvString("a")))
    val listField     = toAvString("listField")     -> AttributeValue.List(Chunk(toAvString("b")))
    val mapField      = toAvString("mapField")      -> AttributeValue.Map(Map(toAvString("k") -> toAvNum(1)))
    val setField      = toAvString("setField")      -> AttributeValue.StringSet(Set("x"))

    val allFields = Map(scalarField, optionalField, chunkField, listField, mapField, setField)

    def itemExcluding(key: String): AttributeValue.Map =
      AttributeValue.Map(allFields - toAvString(key))

    val expectedAllPresent: ListMap[String, _] = ListMap(
      "scalarField"   -> "hello",
      "optionalField" -> Some("present"),
      "chunkField"    -> Chunk("a"),
      "listField"     -> List("b"),
      "mapField"      -> Map("k" -> 1),
      "setField"      -> Set("x")
    )

    def decode(item: AttributeValue.Map) = Codec.decoder(genericRecordAllContainerFieldsSchema)(item)

    suite("genericRecordDecoder missing-field handling (PR #712 follow-up)")(
      test("all fields present decodes normally") {
        assertTrue(decode(AttributeValue.Map(allFields)) == Right(expectedAllPresent))
      },
      test("missing scalarField (Scalar) fails with a decode error, not a silent bad value") {
        assert(decode(itemExcluding("scalarField")))(
          isLeft(equalTo(DecodingError("field 'scalarField' not found in AttributeValue map")))
        )
      },
      test("missing optionalField (Optional) decodes to None") {
        assertTrue(decode(itemExcluding("optionalField")) == Right(expectedAllPresent.updated("optionalField", None)))
      },
      test("missing chunkField (Chunk) decodes to Chunk.empty, not an error") {
        assertTrue(decode(itemExcluding("chunkField")) == Right(expectedAllPresent.updated("chunkField", Chunk.empty)))
      },
      test("missing listField (Sequence) decodes to Nil, not an error") {
        assertTrue(decode(itemExcluding("listField")) == Right(expectedAllPresent.updated("listField", Nil)))
      },
      test("missing mapField (Map) decodes to Map.empty, not an error") {
        assertTrue(decode(itemExcluding("mapField")) == Right(expectedAllPresent.updated("mapField", Map.empty)))
      },
      test("missing setField (Set) decodes to Set.empty, not an error") {
        assertTrue(decode(itemExcluding("setField")) == Right(expectedAllPresent.updated("setField", Set.empty)))
      },
      test(
        "missing List-typed fields on a >22-field case class (arity-triggered GenericRecord fallback) decode to Nil"
      ) {
        // format: off
        val big = Big(
          "id", "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
          "f11", "f12", "f13", "f14", "f15", "f16", "f17", None, None,
          None, None, None, None, None, None, None, None, None, None, None
        )
        val expected = BigList(
          "id", "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
          "f11", "f12", "f13", "f14", "f15", "f16", "f17", Nil, Nil,
          Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil
        )
        // format: on
        val item     = DynamoDBQuery.toItem(big)
        val decoded  = DynamoDBQuery.fromItem[BigList](item)
        assert(decoded)(isRight(equalTo(expected)))
      }
    )
  }

  private def assertEncodesThenDecodesWithGen[A](schema: Schema[A], genA: Gen[Sized, A]) =
    check(genA) { a =>
      assertEncodesThenDecodesPure(schema, a)
    }

  private def assertEncodesThenDecodesPure[A](schema: Schema[A], a: A) = {
    val enc = Codec.encoder(schema)
    val dec = Codec.decoder(schema)

    val encoded = enc(a)
    val decoded = dec(encoded)

    assert(decoded)(isRight(equalTo(a)))
  }

  private def assertEncodesThenDecodes[A](schema: Schema[A], a: A) =
    ZIO.succeed(assertEncodesThenDecodesPure(schema, a))

  case class SearchRequest(query: String, pageNumber: Int, resultPerPage: Int)

  val searchRequestGen: Gen[Sized, SearchRequest] =
    for {
      query      <- Gen.string
      pageNumber <- Gen.int(Int.MinValue, Int.MaxValue)
      results    <- Gen.int(Int.MinValue, Int.MaxValue)
    } yield SearchRequest(query, pageNumber, results)

  val searchRequestSchema: Schema[SearchRequest] = DeriveSchema.gen[SearchRequest]

  sealed trait OneOf
  final case class StringValue(value: String)   extends OneOf
  final case class IntValue(value: Int)         extends OneOf
  final case class BooleanValue(value: Boolean) extends OneOf

  object OneOf {
    implicit val schema: Schema[OneOf] = DeriveSchema.gen[OneOf]
  }

  final case class Enumeration(oneOf: OneOf)
  object Enumeration {
    implicit val schema: Schema[Enumeration] = DeriveSchema.gen[Enumeration]
  }

  sealed trait OneOf2
  case class StringValue2(value: String)                       extends OneOf2
  case class IntValue2(value: Int)                             extends OneOf2
  case class BooleanValue2(value: Boolean)                     extends OneOf2
  case class StringValue2Multi(value1: String, value2: String) extends OneOf2

  case class Enumeration2(oneOf: OneOf2)

  object Enumeration2 {
    implicit val schema: Schema[Enumeration2] = DeriveSchema.gen[Enumeration2]
  }

  case object Singleton
  implicit val schemaObject: Schema[Singleton.type] = DeriveSchema.gen[Singleton.type]

  val nestedRecordSchema: Schema[ListMap[String, _]] = Schema.record(
    TypeId.Structural,
    Schema.Field(
      "l1",
      Schema.Primitive(StandardType.StringType),
      get0 = (p: ListMap[String, _]) => p("l1").asInstanceOf[String],
      set0 = (p: ListMap[String, _], v: String) => p.updated("l1", v)
    ),
    Schema.Field(
      "l2",
      recordSchema,
      get0 = (p: ListMap[String, _]) => p("l2").asInstanceOf[ListMap[String, _]],
      set0 = (p: ListMap[String, _], v: ListMap[String, _]) => p.updated("l2", v)
    )
  )

  final case class Value(first: Int, second: Boolean)
  object Value {
    implicit lazy val schema: Schema[Value] = DeriveSchema.gen[Value]
  }
}
