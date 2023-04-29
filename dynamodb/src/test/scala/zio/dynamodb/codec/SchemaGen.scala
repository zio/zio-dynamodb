package zio.dynamodb.codec

import scala.collection.immutable.ListMap

import zio.Chunk
import zio.test.{ Gen, Sized }
import zio.schema._

object SchemaGen {

  val anyLabel: Gen[Sized, String] = Gen.alphaNumericStringBounded(1, 15)

  def anyStructure[A](
    schemaGen: Gen[Sized, Schema[_]]
  ): Gen[Sized, FieldSet] =
    Gen.setOfBounded(1, 3)(anyLabel).flatMap { keySet =>
      Gen.setOfN(keySet.size)(schemaGen).map { schemas =>
        FieldSet(
          keySet
            .zip(schemas)
            .map {
              case (label, schema) =>
                Schema
                  .Field[ListMap[String, A], A](
                    label,
                    schema.asInstanceOf[Schema[A]],
                    get0 = _(label),
                    set0 = (a, b) => a + (label -> b)
                  )
                  .asInstanceOf[Schema.Field[ListMap[String, _], _]]
            }
            .toSeq: _*
        )
      }
    }

  def anyStructure[A](schema: Schema[A], max: Int = 3): Gen[Sized, Seq[Schema.Field[ListMap[String, _], A]]] =
    Gen
      .setOfBounded(1, max)(
        anyLabel.map(label =>
          Schema.Field[ListMap[String, _], A](
            label,
            schema,
            get0 = _(label).asInstanceOf[A],
            set0 = (a, b) => a + (label -> b)
          )
        )
      )
      .map(_.toSeq)

  def anyEnumeration[A](
    schemaGen: Gen[Sized, Schema[A]]
  ): Gen[Sized, ListMap[String, Schema[A]]] =
    Gen
      .setOfBounded(1, 3)(
        anyLabel.zip(schemaGen)
      )
      .map(ListMap.empty ++ _)

  def anyEnumeration[A](schema: Schema[A]): Gen[Sized, ListMap[String, Schema[A]]] =
    Gen.setOfBounded(1, 3)(anyLabel.map(_ -> schema)).map(ListMap.empty ++ _)

  def anyPrimitive[A]: Gen[Any, Schema.Primitive[A]]                               =
    StandardTypeGen.anyStandardType[A].map(Schema.Primitive(_, Chunk.empty))

  type PrimitiveAndGen[A] = (Schema.Primitive[A], Gen[Sized, A])

  def anyPrimitiveAndGen[A]: Gen[Any, PrimitiveAndGen[A]] =
    StandardTypeGen.anyStandardTypeAndGen[A].map {
      case (standardType, gen) => Schema.Primitive(standardType, Chunk.empty) -> gen
    }

  type PrimitiveAndValue[A] = (Schema.Primitive[A], A)

  def anyPrimitiveAndValue[A]: Gen[Sized, PrimitiveAndValue[A]] =
    for {
      (schema, gen) <- anyPrimitiveAndGen[A]
      value         <- gen
    } yield schema -> value

  def anyOptional[A](
    schemaGen: Gen[Sized, Schema[A]]
  ): Gen[Sized, Schema.Optional[A]]                             =
    schemaGen.map(Schema.Optional(_))

  type OptionalAndGen[A] = (Schema.Optional[A], Gen[Sized, Option[A]])

  def anyOptionalAndGen[A]: Gen[Sized, OptionalAndGen[A]] =
    anyPrimitiveAndGen[A].map {
      case (schema, gen) => Schema.Optional(schema) -> Gen.option(gen)
    }

  type OptionalAndValue[A] = (Schema.Optional[A], Option[A])

  def anyOptionalAndValue[A]: Gen[Sized, OptionalAndValue[A]] =
    for {
      (schema, gen) <- anyOptionalAndGen[A]
      value         <- gen
    } yield schema -> value

  def anyEither[A, B]: Gen[Sized, Schema.Either[A, B]]        =
    for {
      left  <- anyPrimitive[A]
      right <- anyPrimitive[B]
    } yield Schema.Either(left, right)

  type EitherAndGen[A, B] = (Schema.Either[A, B], Gen[Sized, scala.util.Either[A, B]])

  def anyEitherAndGen[A, B]: Gen[Sized, EitherAndGen[A, B]] =
    for {
      (leftSchema, leftGen)   <- anyPrimitiveAndGen[A]
      (rightSchema, rightGen) <- anyPrimitiveAndGen[B]
    } yield (Schema.Either(leftSchema, rightSchema), Gen.either(leftGen, rightGen))

  type EitherAndValue[A, B] = (Schema.Either[A, B], scala.util.Either[A, B])

  def anyEitherAndValue[A, B]: Gen[Sized, EitherAndValue[A, B]] =
    for {
      (schema, gen) <- anyEitherAndGen[A, B]
      value         <- gen
    } yield (schema, value)

  def anyTuple[A, B]: Gen[Sized, Schema.Tuple2[A, B]] =
    anySchema[A].zipWith(anySchema[B]) { (a, b) =>
      Schema.Tuple2(a, b)
    }

  type TupleAndGen[A, B] = (Schema.Tuple2[A, B], Gen[Sized, (A, B)])

  def anyTupleAndGen[A, B]: Gen[Sized, TupleAndGen[A, B]] =
    for {
      (schemaA, genA) <- anyPrimitiveAndGen[A]
      (schemaB, genB) <- anyPrimitiveAndGen[B]
    } yield Schema.Tuple2(schemaA, schemaB) -> genA.zip(genB)

  type TupleAndValue[A, B] = (Schema.Tuple2[A, B], (A, B))

  def anyTupleAndValue[A, B]: Gen[Sized, TupleAndValue[A, B]] =
    for {
      (schema, gen)    <- anyTupleAndGen[A, B]
      (valueA, valueB) <- gen
    } yield schema -> ((valueA, valueB))

  def anySequence[A]: Gen[Sized, Schema[Chunk[A]]]            =
    anySchema[A].map(Schema.chunk(_))

  type SequenceAndGen[A] = (Schema[Chunk[A]], Gen[Sized, Chunk[A]])

  def anySequenceAndGen[A]: Gen[Sized, SequenceAndGen[A]] =
    anyPrimitiveAndGen[A].map {
      case (schema, gen) =>
        Schema.chunk(schema) -> Gen.chunkOf(gen)
    }

  type SequenceAndValue[A] = (Schema[Chunk[A]], Chunk[A])

  def anySequenceAndValue[A]: Gen[Sized, SequenceAndValue[A]]         =
    for {
      (schema, gen) <- anySequenceAndGen[A]
      value         <- gen
    } yield schema -> value

  def toCaseSet[A](cases: ListMap[String, Schema[_]]): CaseSet.Aux[A] =
    cases.foldRight[CaseSet.Aux[A]](CaseSet.Empty[A]()) {
      case ((id, codec), acc) =>
        val _case = Schema.Case[A, Any](
          id,
          codec.asInstanceOf[Schema[Any]],
          _.asInstanceOf[Any],
          _.asInstanceOf[A],
          _.isInstanceOf[Any],
          Chunk.empty
        )
        CaseSet.Cons(_case, acc)
    }

  def anyMap[K, V]: Gen[Sized, Schema.Map[K, V]] =
    anySchema[K].zipWith(anySchema[V]) { (a, b) =>
      Schema.Map(a, b, Chunk.empty)
    }
  type MapAndGen[K, V] = (Schema.Map[K, V], Gen[Sized, Map[K, V]])

  def anyMapAndGen[K, V]: Gen[Sized, MapAndGen[K, V]] =
    for {
      (schemaK, genK) <- anyPrimitiveAndGen[K]
      (schemaV, genV) <- anyPrimitiveAndGen[V]
    } yield Schema.Map(schemaK, schemaV, Chunk.empty) -> (genK.flatMap(k =>
      genV.map(v => scala.collection.immutable.Map(k -> v))
    )
    )

  type MapAndValue[K, V] = (Schema.Map[K, V], Map[K, V])

  def anyMapAndValue[K, V]: Gen[Sized, MapAndValue[K, V]] =
    for {
      (schema, gen) <- anyMapAndGen[K, V]
      map           <- gen
    } yield schema -> map

  type SetAndGen[A] = (Schema.Set[A], Gen[Sized, scala.collection.immutable.Set[A]])

  def anySetAndGen[A]: Gen[Sized, SetAndGen[A]] =
    anyPrimitiveAndGen[A].map {
      case (schema, gen) =>
        Schema.Set(schema, Chunk.empty) -> Gen.setOf(gen)
    }

  type SetAndValue[A] = (Schema.Set[A], scala.collection.immutable.Set[A])

  def anySetAndValue[A]: Gen[Sized, SetAndValue[A]] =
    for {
      (schema, gen) <- anySetAndGen[A]
      value         <- gen
    } yield schema -> value

  def anyEnumeration[A]: Gen[Sized, Schema[A]]      =
    for {
      caseSet <- anyEnumeration[A](anySchema[A]).map(toCaseSet[A])
      id      <- Gen.string(Gen.alphaChar).map(TypeId.parse)
    } yield Schema.enumeration[A, CaseSet.Aux[A]](id, caseSet)

  type EnumerationAndGen = (Schema[Any], Gen[Sized, Any])

  def anyEnumerationAndGen[A]: Gen[Sized, EnumerationAndGen] =
    for {
      primitiveAndGen <- anyPrimitiveAndGen[A]
      structure       <- anyEnumeration(primitiveAndGen._1)
      primitiveValue  <- primitiveAndGen._2
      id              <- Gen.string(Gen.alphaChar).map(TypeId.parse)
    } yield {
      val gen = Gen.oneOf(structure.keys.map(Gen.const(_)).toSeq: _*).map(_ => primitiveValue)
      Schema.enumeration[Any, CaseSet.Aux[Any]](id, toCaseSet(structure)) -> gen
    }

  type EnumerationAndValue = (Schema[Any], Any)

  val anyEnumerationAndValue: Gen[Sized, EnumerationAndValue] =
    for {
      (schema, gen) <- anyEnumerationAndGen
      value         <- gen
    } yield schema -> value

  val anyRecord: Gen[Sized, Schema[ListMap[String, _]]]       =
    for {
      name     <- Gen.string(Gen.alphaChar).map(TypeId.parse)
      fieldSet <- anyStructure(anySchema)
    } yield Schema.record(name, fieldSet)

  type GenericRecordAndGen = (Schema[ListMap[String, _]], Gen[Sized, ListMap[String, _]])

  def anyGenericRecordAndGen(maxFieldCount: Int = 3): Gen[Sized, GenericRecordAndGen] =
    for {
      (schema, gen) <- anyPrimitiveAndGen[Any]
      name          <- Gen.string(Gen.alphaChar).map(TypeId.parse)
      structure     <- anyStructure(schema, maxFieldCount)
    } yield {
      val valueGen: Gen[Sized, ListMap[String, _]] =
        Gen
          .const(structure.map(_.name.toString()))
          .zip(Gen.listOfN(structure.size)(gen))
          .map {
            case (labels, values) =>
              labels.zip(values)
          }
          .map(ListMap.empty ++ _)

      Schema.record(name, structure: _*) -> valueGen
    }

  type RecordAndValue = (Schema[ListMap[String, _]], ListMap[String, _])

  def anyRecordAndValue(maxFieldCount: Int = 3): Gen[Sized, RecordAndValue] =
    for {
      (schema, gen) <- anyGenericRecordAndGen(maxFieldCount)
      value         <- gen
    } yield schema -> value

  val anyRecordOfRecordsAndValue: Gen[Sized, RecordAndValue]                =
    for {
      (schema1, gen1) <- anyGenericRecordAndGen()
      (schema2, gen2) <- anyGenericRecordAndGen()
      (schema3, gen3) <- anyGenericRecordAndGen()
      name            <- Gen.string(Gen.alphaChar).map(TypeId.parse)
      keys            <- Gen.setOfN(3)(anyLabel).map(_.toSeq)
      (key1, value1)  <- Gen.const(keys(0)).zip(gen1)
      (key2, value2)  <- Gen.const(keys(1)).zip(gen2)
      (key3, value3)  <- Gen.const(keys(2)).zip(gen3)
    } yield Schema.record(
      name,
      FieldSet(
        Schema.Field(key1, schema1.asInstanceOf[Schema[Any]], get0 = _(key1), set0 = (r, v: Any) => r.updated(key1, v)),
        Schema.Field(key2, schema2.asInstanceOf[Schema[Any]], get0 = _(key2), set0 = (r, v: Any) => r.updated(key2, v)),
        Schema.Field(key3, schema3.asInstanceOf[Schema[Any]], get0 = _(key3), set0 = (r, v: Any) => r.updated(key3, v))
      )
    ) -> ListMap(
      (key1, value1),
      (key2, value2),
      (key3, value3)
    )

  type SequenceTransform[A] = Schema.Transform[Chunk[A], List[A], String]

  def anySequenceTransform[A]: Gen[Sized, SequenceTransform[A]] =
    anySequence[A].map(schema => transformSequence(schema))

  type SequenceTransformAndGen[A] = (SequenceTransform[A], Gen[Sized, List[A]])

  def anySequenceTransformAndGen[A]: Gen[Sized, SequenceTransformAndGen[A]] =
    anyPrimitiveAndGen[A].map {
      case (schema, gen) =>
        transformSequence(Schema.chunk(schema)) -> Gen.listOf(gen)
    }

  // TODO: Add some random Left values.
  private def transformSequence[A](schema: Schema[Chunk[A]]): SequenceTransform[A] =
    Schema.Transform[Chunk[A], List[A], String](
      schema,
      chunk => Right(chunk.toList),
      list => Right(Chunk.fromIterable(list)),
      Chunk.empty,
      "transformSequence"
    )

  type SequenceTransformAndValue[A] = (SequenceTransform[A], List[A])

  def anySequenceTransformAndValue[A]: Gen[Sized, SequenceTransformAndValue[A]] =
    for {
      (schema, gen) <- anySequenceTransformAndGen[A]
      value         <- gen
    } yield schema -> value

  type RecordTransform[A] = Schema.Transform[ListMap[String, _], A, String]

  def anyRecordTransform[A]: Gen[Sized, RecordTransform[A]] =
    anyRecord.map(schema => transformRecord(schema))

  type RecordTransformAndGen[A] = (RecordTransform[A], Gen[Sized, A])

  // TODO: How do we generate a value of a type that we know nothing about?
  def anyRecordTransformAndGen[A]: Gen[Sized, RecordTransformAndGen[A]] =
    Gen.empty
  //    anyRecordAndGen.map {
  //      case (schema, gen) => transformRecord(schema) -> gen
  //    }

  // TODO: Dynamically generate a case class.
  def transformRecord[A](schema: Schema[ListMap[String, _]]): RecordTransform[A] =
    Schema.Transform[ListMap[String, _], A, String](
      schema,
      _ => Left("Not implemented."),
      _ => Left("Not implemented."),
      Chunk.empty,
      "transformRecord"
    )

  type RecordTransformAndValue[A] = (RecordTransform[A], A)

  def anyRecordTransformAndValue[A]: Gen[Sized, RecordTransformAndValue[A]] =
    for {
      (schema, gen) <- anyRecordTransformAndGen[A]
      value         <- gen
    } yield schema -> value

  type EnumerationTransform[A] = Schema.Transform[Any, A, String]

  def anyEnumerationTransform[A]: Gen[Sized, EnumerationTransform[A]] =
    anyEnumeration.map(schema => transformEnumeration[A](schema))

  type EnumerationTransformAndGen[A] = (EnumerationTransform[A], Gen[Sized, A])

  // TODO: How do we generate a value of a type that we know nothing about?
  def anyEnumerationTransformAndGen[A]: Gen[Sized, EnumerationTransformAndGen[A]] =
    Gen.empty
  //    anyEnumerationAndGen.map {
  //      case (schema, gen) => transformEnumeration(schema) -> gen
  //    }

  // TODO: Dynamically generate a sealed trait and case/value classes.
  def transformEnumeration[A](schema: Schema[Any]): EnumerationTransform[A] =
    Schema.Transform[Any, A, String](
      schema,
      _ => Left("Not implemented."),
      _ => Left("Not implemented."),
      Chunk.empty,
      "transformEnumeration"
    )

  type EnumerationTransformAndValue[A] = (EnumerationTransform[A], A)

  def anyEnumerationTransformAndValue[A]: Gen[Sized, EnumerationTransformAndValue[A]] =
    for {
      (schema, gen) <- anyEnumerationTransformAndGen[A]
      value         <- gen
    } yield schema -> value

  def anyTransform[A]: Gen[Sized, Schema.Transform[Any, A, String]]                   =
    Gen.oneOf(
      anySequenceTransform[A].asInstanceOf[Gen[Sized, Schema.Transform[Any, A, String]]],
      anyRecordTransform[A].asInstanceOf[Gen[Sized, Schema.Transform[Any, A, String]]],
      anyEnumerationTransform[A].asInstanceOf[Gen[Sized, Schema.Transform[Any, A, String]]]
    )

  type TransformAndValue[A] = (Schema.Transform[_, A, String], A)

  def anyTransformAndValue[A]: Gen[Sized, TransformAndValue[A]] =
    Gen.oneOf[Sized, TransformAndValue[A]](
      anySequenceTransformAndValue[A].asInstanceOf[Gen[Sized, TransformAndValue[A]]]
      // anyRecordTransformAndValue,
      // anyEnumerationTransformAndValue
    )

  type TransformAndGen[A] = (Schema.Transform[_, A, String], Gen[Sized, A])

  def anyTransformAndGen[A]: Gen[Sized, TransformAndGen[A]] =
    Gen.oneOf[Sized, TransformAndGen[A]](
      anySequenceTransformAndGen[A].asInstanceOf[Gen[Sized, TransformAndGen[A]]],
      anyRecordTransformAndGen[A],
      anyEnumerationTransformAndGen[A]
    )

  def anySchema[A]: Gen[Sized, Schema[A]] =
    for {
      treeDepth <- Gen.bounded(0, 2)(Gen.const(_))
      tree      <- anyTree[A](treeDepth)
    } yield tree

  def anyValueForSchema[A](schema: Schema[A]): Gen[Sized, (Schema[A], A)] =
    DynamicValueGen
      .anyDynamicValueOfSchema(schema)
      .map { dynamic =>
        schema -> schema.fromDynamic(dynamic).toOption.get
      }

  type SchemaAndValue[A] = (Schema[A], A)

  def anySchemaAndValue[A]: Gen[Sized, SchemaAndValue[A]] =
    for {
      schema  <- anySchema[A]
      dynamic <- DynamicValueGen.anyDynamicValueOfSchema(schema)
    } yield (schema -> schema.fromDynamic(dynamic).toOption.get).asInstanceOf[SchemaAndValue[A]]

  sealed trait Arity
  case object Arity0 extends Arity
  final case class Arity1(value: Int) extends Arity

  object Arity1 {
    implicit val schema: Schema[Arity1] = DeriveSchema.gen[Arity1]
  }
  final case class Arity2(value1: String, value2: Arity1) extends Arity

  object Arity2 {
    implicit val schema: Schema[Arity2] = DeriveSchema.gen[Arity2]
  }
  final case class Arity3(value1: String, value2: Arity2, value3: Arity1) extends Arity

  object Arity3 {
    implicit val schema: Schema[Arity3] = DeriveSchema.gen[Arity3]
  }
  final case class Arity24(
    a1: Arity1,
    a2: Arity2,
    a3: Arity3,
    f4: Int = 4,
    f5: Int = 5,
    f6: Int = 6,
    f7: Int = 7,
    f8: Int = 8,
    f9: Int = 9,
    f10: Int = 10,
    f11: Int = 11,
    f12: Int = 12,
    f13: Int = 13,
    f14: Int = 14,
    f15: Int = 15,
    f16: Int = 16,
    f17: Int = 17,
    f18: Int = 18,
    f19: Int = 19,
    f20: Int = 20,
    f21: Int = 21,
    f22: Int = 22,
    f23: Int = 23,
    f24: Int = 24
  ) extends Arity

  object Arity24 {
    implicit val schema: Schema[Arity24] = DeriveSchema.gen[Arity24]
  }

  object Arity {
    implicit val arityEnumSchema: Schema.Enum[Arity] = DeriveSchema.gen[Arity].asInstanceOf[Schema.Enum[Arity]]
  }

  lazy val anyArity1: Gen[Sized, Arity1] = Gen.int.map(Arity1(_))

  lazy val anyArity2: Gen[Sized, Arity2] =
    for {
      s  <- Gen.string
      a1 <- anyArity1
    } yield Arity2(s, a1)

  lazy val anyArity3: Gen[Sized, Arity3] =
    for {
      s  <- Gen.string
      a1 <- anyArity1
      a2 <- anyArity2
    } yield Arity3(s, a2, a1)

  lazy val anyArity24: Gen[Sized, Arity24] =
    for {
      a1 <- anyArity1
      a2 <- anyArity2
      a3 <- anyArity3
    } yield Arity24(a1, a2, a3)

  lazy val anyArity: Gen[Sized, Arity] = Gen.oneOf(anyArity1, anyArity2, anyArity3, anyArity24)

  type CaseClassAndGen[A] = (Schema[A], Gen[Sized, A])

  type CaseClassAndValue[A] = (Schema[A], A)

  lazy val anyCaseClassSchema: Gen[Sized, Schema[_]] =
    Gen.oneOf(
      Gen.const(Schema[Arity1]),
      Gen.const(Schema[Arity2]),
      Gen.const(Schema[Arity3]),
      Gen.const(Schema[Arity24])
    )

  def anyCaseClassAndGen[A]: Gen[Sized, CaseClassAndGen[A]] =
    anyCaseClassSchema.map {
      case s @ Schema.CaseClass1(_, _, _, _)       => (s -> anyArity1).asInstanceOf[CaseClassAndGen[A]]
      case s @ Schema.CaseClass2(_, _, _, _, _)    => (s -> anyArity2).asInstanceOf[CaseClassAndGen[A]]
      case s @ Schema.CaseClass3(_, _, _, _, _, _) => (s -> anyArity3).asInstanceOf[CaseClassAndGen[A]]
      case s                                       => (s -> anyArity24).asInstanceOf[CaseClassAndGen[A]]
    }

  def anyCaseClassAndValue[A]: Gen[Sized, CaseClassAndValue[A]] =
    for {
      (schema, gen) <- anyCaseClassAndGen[A]
      value         <- gen
    } yield (schema -> value)

  type EnumAndGen[A] = (Schema[A], Gen[Sized, A])

  type EnumAndValue[A] = (Schema[A], A)

  lazy val anyEnumSchema: Gen[Any, Schema.Enum[Arity]] = Gen.const(Arity.arityEnumSchema)

  def anyEnumAndGen[A]: Gen[Sized, EnumAndGen[A]]     =
    anyEnumSchema.map(_ -> anyArity).asInstanceOf[Gen[Sized, EnumAndGen[A]]]

  def anyEnumAndValue[A]: Gen[Sized, EnumAndValue[A]] =
    for {
      (schema, gen) <- anyEnumAndGen[A]
      value         <- gen
    } yield schema -> value

  def anyLeaf[A]: Gen[Sized, Schema[A]]               =
    Gen.oneOf(
      anyPrimitive[A],
      anyPrimitive[A].map(Schema.list(_).asInstanceOf[Schema[A]]),
      anyPrimitive[A].map(_.optional.asInstanceOf[Schema[A]]),
      anyPrimitive[A].zip(anyPrimitive[A]).map { case (l, r) => Schema.either(l, r).asInstanceOf[Schema[A]] },
      anyPrimitive[A].zip(anyPrimitive[A]).map { case (l, r) => Schema.tuple2(l, r).asInstanceOf[Schema[A]] },
      anyStructure(anyPrimitive)
        .zip(Gen.string(Gen.alphaChar).map(TypeId.parse))
        .map(z => Schema.record(z._2, z._1).asInstanceOf[Schema[A]]),
      Gen.const(Schema[Json]).asInstanceOf[Gen[Sized, Schema[A]]],
      anyCaseClassSchema.asInstanceOf[Gen[Sized, Schema[A]]],
      anyEnumSchema.asInstanceOf[Gen[Sized, Schema[A]]]
    )

  def anyTree[A](depth: Int): Gen[Sized, Schema[A]] =
    if (depth == 0)
      anyLeaf.asInstanceOf[Gen[Sized, Schema[A]]]
    else
      Gen
        .oneOf(
          anyTree[A](depth - 1).map(Schema.list(_)),
          // Nested optional cause some issues. Ignore them for now: See https://github.com/zio/zio-schema/issues/68
          anyTree[A](depth - 1).map {
            case s @ Schema.Optional(_, _) => s
            case s                         => Schema.option(s)
          },
          anyTree[A](depth - 1).zip(anyTree[A](depth - 1)).map { case (l, r) => Schema.either(l, r) },
          anyTree[A](depth - 1).zip(anyTree[A](depth - 1)).map { case (l, r) => Schema.tuple2(l, r) },
          anyStructure(anyTree(depth - 1))
            .zip(Gen.string(Gen.alphaChar).map(TypeId.parse))
            .map(z => Schema.record(z._2, z._1)),
          Gen.const(Schema[Json]),
          anyDynamic
        )
        .asInstanceOf[Gen[Sized, Schema[A]]]

  type SchemaAndDerivedValue[A, B] = (Schema[A], Schema[B], Chunk[scala.util.Either[A, B]])

  def anyLeafAndValue[A]: Gen[Sized, SchemaAndValue[A]] =
    for {
      schema <- anyLeaf[A]
      value  <- DynamicValueGen.anyDynamicValueOfSchema(schema)
    } yield (schema -> schema.fromDynamic(value).toOption.get)

  def anyTreeAndValue[A]: Gen[Sized, SchemaAndValue[A]] =
    for {
      schema <- anyTree[A](1)
      value  <- DynamicValueGen.anyDynamicValueOfSchema(schema)
    } yield (schema -> schema.fromDynamic(value).toOption.get)

  sealed trait Json
  case object JNull extends Json
  case class JString(s: String)                    extends Json
  case class JNumber(l: Int)                       extends Json
  case class JDecimal(d: Double)                   extends Json
  case class JObject(fields: List[(String, Json)]) extends Json
  case class JArray(fields: List[Json])            extends Json

  object Json {
    implicit lazy val schema: Schema.Enum6[_, _, _, _, _, _, Json] =
      DeriveSchema.gen[Json]

    val leafGen: Gen[Sized, Json] =
      Gen.oneOf(
        Gen.const(JNull),
        Gen.string.map(JString(_)),
        Gen.int.map(JNumber(_))
      )

    val gen: Gen[Sized, Json] =
      for {
        keys   <- Gen.setOfN(3)(Gen.string)
        values <- Gen.setOfN(3)(leafGen)
      } yield JObject(keys.zip(values).toList)

    val genDeep: Gen[Sized, Json] =
      Gen.sized { max =>
        Gen.bounded(1, max) { depth =>
          leafGen.map { leaf =>
            (1 to depth).foldLeft(leaf) {
              case (gen, n) =>
                JObject(List(n.toString -> gen))
            }
          }
        }
      }
  }

  def anyRecursiveType[A]: Gen[Sized, Schema[A]] =
    Gen.const(Schema[Json]).asInstanceOf[Gen[Sized, Schema[A]]]

  def anyRecursiveTypeAndValue[A]: Gen[Sized, SchemaAndValue[A]] =
    for {
      schema <- Gen.const(Schema[Json])
      value  <- Json.gen
    } yield (schema.asInstanceOf[Schema[A]], value.asInstanceOf[A])

  def anyDeepRecursiveTypeAndValue[A]: Gen[Sized, SchemaAndValue[A]] =
    for {
      schema <- Gen.const(Schema[Json])
      value  <- Json.genDeep
    } yield (schema.asInstanceOf[Schema[A]], value.asInstanceOf[A])

  lazy val anyDynamic: Gen[Any, Schema[DynamicValue]] = Gen.const(Schema.dynamicValue)

  case class SchemaTest[A](name: String, schema: StandardType[A], gen: Gen[Sized, A])

  def schemasAndGens: List[SchemaTest[_]] =
    List(
      SchemaTest("String", StandardType.StringType, Gen.string),
      SchemaTest("Bool", StandardType.BoolType, Gen.boolean),
      SchemaTest("Short", StandardType.ShortType, Gen.short),
      SchemaTest("Int", StandardType.IntType, Gen.int),
      SchemaTest("Long", StandardType.LongType, Gen.long),
      SchemaTest("Float", StandardType.FloatType, Gen.float),
      SchemaTest("Double", StandardType.DoubleType, Gen.double),
      SchemaTest("Binary", StandardType.BinaryType, Gen.chunkOf(Gen.byte)),
      SchemaTest("Char", StandardType.CharType, Gen.asciiChar),
      SchemaTest("UUID", StandardType.UUIDType, Gen.uuid),
      SchemaTest(
        "BigDecimal",
        StandardType.BigDecimalType,
        Gen.double.map(d => java.math.BigDecimal.valueOf(d))
      ),
      SchemaTest(
        "BigInteger",
        StandardType.BigIntegerType,
        Gen.long.map(n => java.math.BigInteger.valueOf(n))
      ),
      SchemaTest("DayOfWeek", StandardType.DayOfWeekType, JavaTimeGen.anyDayOfWeek),
      SchemaTest("Duration", StandardType.DurationType, JavaTimeGen.anyDuration),
      SchemaTest("Instant", StandardType.InstantType, JavaTimeGen.anyInstant),
      SchemaTest("LocalDate", StandardType.LocalDateType, JavaTimeGen.anyLocalDate),
      SchemaTest(
        "LocalDateTime",
        StandardType.LocalDateTimeType,
        JavaTimeGen.anyLocalDateTime
      ),
      SchemaTest(
        "LocalTime",
        StandardType.LocalTimeType,
        JavaTimeGen.anyLocalTime
      ),
      SchemaTest("Month", StandardType.MonthType, JavaTimeGen.anyMonth),
      SchemaTest("MonthDay", StandardType.MonthDayType, JavaTimeGen.anyMonthDay),
      SchemaTest(
        "OffsetDateTime",
        StandardType.OffsetDateTimeType,
        JavaTimeGen.anyOffsetDateTime
      ),
      SchemaTest(
        "OffsetTime",
        StandardType.OffsetTimeType,
        JavaTimeGen.anyOffsetTime
      ),
      SchemaTest("Period", StandardType.PeriodType, JavaTimeGen.anyPeriod),
      SchemaTest("Year", StandardType.YearType, JavaTimeGen.anyYear),
      SchemaTest("YearMonth", StandardType.YearMonthType, JavaTimeGen.anyYearMonth),
      SchemaTest(
        "ZonedDateTime",
        StandardType.ZonedDateTimeType,
        JavaTimeGen.anyZonedDateTime
      ),
      SchemaTest("ZoneId", StandardType.ZoneIdType, JavaTimeGen.anyZoneId),
      SchemaTest("ZoneOffset", StandardType.ZoneOffsetType, JavaTimeGen.anyZoneOffset),
      SchemaTest("UnitType", StandardType.UnitType, Gen.unit)
    )

}
