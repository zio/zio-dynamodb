package zio.dynamodb.codec

import zio.Chunk
import zio.dynamodb.{ AttributeValue, ToAttributeValue }
import zio.schema.Schema.chunk
import zio.schema.{ DeriveSchema, Schema, StandardType }

import java.time.Instant
import java.time.format.DateTimeFormatter

trait CodecTestFixtures {

  lazy implicit val nestedCaseClass2Schema: Schema[NestedCaseClass2]               = DeriveSchema.gen[NestedCaseClass2]
  lazy implicit val simpleCaseClass3Schema: Schema[SimpleCaseClass3]               = DeriveSchema.gen[SimpleCaseClass3]
  lazy implicit val simpleCaseClass3SchemaOptional: Schema[SimpleCaseClass3Option] =
    DeriveSchema.gen[SimpleCaseClass3Option]
  lazy implicit val caseClassOfList: Schema[CaseClassOfList]                       = DeriveSchema.gen[CaseClassOfList]
  lazy implicit val caseClassOfListOfCaseClass: Schema[CaseClassOfListOfCaseClass] =
    DeriveSchema.gen[CaseClassOfListOfCaseClass]
  lazy implicit val caseClassOfOption: Schema[CaseClassOfOption]                   = DeriveSchema.gen[CaseClassOfOption]
  lazy implicit val caseClassOfNestedOption: Schema[CaseClassOfNestedOption]       = DeriveSchema.gen[CaseClassOfNestedOption]
  lazy implicit val caseClassOfEither: Schema[CaseClassOfEither]                   = DeriveSchema.gen[CaseClassOfEither]
  lazy implicit val caseClassOfTuple3: Schema[CaseClassOfTuple3]                   = DeriveSchema.gen[CaseClassOfTuple3]
  lazy implicit val instantSchema: Schema.Primitive[Instant]                       =
    Schema.Primitive(StandardType.Instant(DateTimeFormatter.ISO_INSTANT))
  lazy implicit val caseClassOfInstant: Schema[CaseClassOfInstant]                 = DeriveSchema.gen[CaseClassOfInstant]
  lazy implicit val caseClassOfStatus: Schema[CaseClassOfStatus]                   = DeriveSchema.gen[CaseClassOfStatus]

  implicit def myDodgyMap[String, V](implicit element: Schema[(String, V)]): Schema[Map[String, V]] = {
    val value: Schema[Chunk[(String, V)]] = chunk(element)
    value.transform(_.toMap[String, V], Chunk.fromIterable(_))
  }

//  implicit def myDodgyMap2[String, V](implicit element: Schema[(String, V)]): Schema[Map[String, V]] = {
//    val value: Schema[Chunk[(String, V)]] = record(element)
//    value.transform(_.toMap[String, V], Chunk.fromIterable(_))
//  }
  lazy implicit val caseClassOfMapOfInt: Schema[CaseClassOfMapOfInt] = DeriveSchema.gen[CaseClassOfMapOfInt]

  implicit val caseClassOfListOfTuple2: Schema[CaseClassOfListOfTuple2] = DeriveSchema.gen[CaseClassOfListOfTuple2]

  implicit val statusSchema: Schema[Status] = DeriveSchema.gen[Status]

  def toNum(i: Int): AttributeValue.Number                                               = AttributeValue.Number(BigDecimal(i))
  def toList(xs: AttributeValue*): AttributeValue.List                                   = AttributeValue.List(xs.toList)
  def toTuple[A: ToAttributeValue, B: ToAttributeValue](a: A, b: B): AttributeValue.List =
    toList(ToAttributeValue[A].toAttributeValue(a), ToAttributeValue[B].toAttributeValue(b))
}
