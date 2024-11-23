package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, ToAttributeValue }
import zio.schema.CaseSet.caseOf
import zio.schema.{ CaseSet, DeriveSchema, Schema, StandardType }

import java.time.Instant
import scala.collection.immutable.ListMap
import zio.schema.TypeId

trait CodecTestFixtures {

  val recordSchema: Schema[ListMap[String, _]] = Schema.record(
    TypeId.Structural,
    Schema.Field(
      "foo",
      Schema.Primitive(StandardType.StringType),
      get0 = (p: ListMap[String, _]) => p("foo").asInstanceOf[String],
      set0 = (p: ListMap[String, _], v: String) => p.updated("foo", v)
    ),
    Schema
      .Field(
        "bar",
        Schema.Primitive(StandardType.IntType),
        get0 = (p: ListMap[String, _]) => p("bar").asInstanceOf[Int],
        set0 = (p: ListMap[String, _], v: Int) => p.updated("bar", v)
      )
  )

  val enumSchema: Schema[Any] = Schema.enumeration[Any, CaseSet.Aux[Any]](
    TypeId.Structural,
    caseOf[String, Any]("string")(_.asInstanceOf[String])(_.asInstanceOf[Any])(_.isInstanceOf[String]) ++ caseOf[
      Int,
      Any
    ]("int")(_.asInstanceOf[Int])(_.asInstanceOf[Any])(_.isInstanceOf[Int]) ++ caseOf[
      Boolean,
      Any
    ]("boolean")(_.asInstanceOf[Boolean])(_.asInstanceOf[Any])(_.isInstanceOf[Boolean])
  )

  lazy implicit val caseClassOfCurrencySchema: Schema[CaseClassOfCurrency]         = DeriveSchema.gen[CaseClassOfCurrency]
  lazy implicit val nestedCaseClass2Schema: Schema[NestedCaseClass2]               = DeriveSchema.gen[NestedCaseClass2]
  lazy implicit val simpleCaseClass3Schema: Schema[SimpleCaseClass3]               = DeriveSchema.gen[SimpleCaseClass3]
  lazy implicit val simpleCaseClass3SchemaOptional: Schema[SimpleCaseClass3Option] =
    DeriveSchema.gen[SimpleCaseClass3Option]
  lazy implicit val caseClassOfChunk: Schema[CaseClassOfChunk]                     = DeriveSchema.gen[CaseClassOfChunk]
  lazy implicit val caseClassOfList: Schema[CaseClassOfList]                       = DeriveSchema.gen[CaseClassOfList]
  lazy implicit val caseClassOfListOfCaseClass: Schema[CaseClassOfListOfCaseClass] =
    DeriveSchema.gen[CaseClassOfListOfCaseClass]
  lazy implicit val caseClassOfOption: Schema[CaseClassOfOption]                   = DeriveSchema.gen[CaseClassOfOption]
  lazy implicit val caseClassOfNestedOption: Schema[CaseClassOfNestedOption]       = DeriveSchema.gen[CaseClassOfNestedOption]
  lazy implicit val caseClassOfEither: Schema[CaseClassOfEither]                   = DeriveSchema.gen[CaseClassOfEither]
  lazy implicit val caseClassOfTuple2: Schema[CaseClassOfTuple2]                   = DeriveSchema.gen[CaseClassOfTuple2]
  lazy implicit val caseClassOfTuple3: Schema[CaseClassOfTuple3]                   = DeriveSchema.gen[CaseClassOfTuple3]
  lazy implicit val instantSchema: Schema.Primitive[Instant]                       =
    Schema.Primitive(StandardType.InstantType)
  lazy implicit val caseClassOfInstant: Schema[CaseClassOfInstant]                 = DeriveSchema.gen[CaseClassOfInstant]
  lazy implicit val caseClassOfStatus: Schema[CaseClassOfStatus]                   = DeriveSchema.gen[CaseClassOfStatus]

  implicit val caseClassOfMapOfInt: Schema[CaseClassOfMapOfInt] = DeriveSchema.gen[CaseClassOfMapOfInt]

  implicit val caseClassOfSetOfInt: Schema[CaseClassOfSetOfInt] = DeriveSchema.gen[CaseClassOfSetOfInt]

  implicit val caseClassOfListOfTuple2: Schema[CaseClassOfListOfTuple2] = DeriveSchema.gen[CaseClassOfListOfTuple2]

  implicit val statusSchema: Schema[Status] = DeriveSchema.gen[Status]

  def toAvString(s: String): AttributeValue.String                                         = AttributeValue.String(s)
  def toAvNum(i: Int): AttributeValue.Number                                               = AttributeValue.Number(BigDecimal(i))
  def toAvList(xs: AttributeValue*): AttributeValue.List                                   = AttributeValue.List(xs.toList)
  def toAvTuple[A: ToAttributeValue, B: ToAttributeValue](a: A, b: B): AttributeValue.List =
    toAvList(ToAttributeValue[A].toAttributeValue(a), ToAttributeValue[B].toAttributeValue(b))
}
