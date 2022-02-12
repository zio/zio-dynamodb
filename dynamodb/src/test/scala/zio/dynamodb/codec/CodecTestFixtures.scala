package zio.dynamodb.codec

import zio.Chunk
import zio.dynamodb.{ AttributeValue, ToAttributeValue }
import zio.schema.CaseSet.caseOf
import zio.schema.Schema.chunk
import zio.schema.{ CaseSet, DeriveSchema, Schema, StandardType }

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

trait CodecTestFixtures {

  val recordSchema: Schema[ListMap[String, _]] = Schema.record(
    Schema.Field("foo", Schema.Primitive(StandardType.StringType)),
    Schema.Field("bar", Schema.Primitive(StandardType.IntType))
  )

  val enumSchema: Schema[Any] = Schema.enumeration[Any, CaseSet.Aux[Any]](
    caseOf[String, Any]("string")(_.asInstanceOf[String]) ++ caseOf[Int, Any]("int")(_.asInstanceOf[Int]) ++ caseOf[
      Boolean,
      Any
    ]("boolean")(_.asInstanceOf[Boolean])
  )

  lazy implicit val nestedCaseClass2Schema: Schema[NestedCaseClass2]               = DeriveSchema.gen[NestedCaseClass2]
  lazy implicit val simpleCaseClass3Schema: Schema[SimpleCaseClass3]               = DeriveSchema.gen[SimpleCaseClass3]
  lazy implicit val simpleCaseClass3SchemaOptional: Schema[SimpleCaseClass3Option] =
    DeriveSchema.gen[SimpleCaseClass3Option]
  lazy implicit val caseClassOfList: Schema[CaseClassOfList]                       = DeriveSchema.gen[CaseClassOfList]
  lazy implicit val caseClassOfListOfCaseClass: Schema[CaseClassOfListOfCaseClass] =
    DeriveSchema.gen[CaseClassOfListOfCaseClass]
  lazy implicit val caseClassOfOption: Schema[CaseClassOfOption]                   = DeriveSchema.gen[CaseClassOfOption]
  lazy implicit val caseClass2OfOption: Schema[CaseClass2OfOption]                 = DeriveSchema.gen[CaseClass2OfOption]
  lazy implicit val caseClassOfNestedOption: Schema[CaseClassOfNestedOption]       = DeriveSchema.gen[CaseClassOfNestedOption]
  lazy implicit val caseClassOfEither: Schema[CaseClassOfEither]                   = DeriveSchema.gen[CaseClassOfEither]
  lazy implicit val caseClassOfTuple2: Schema[CaseClassOfTuple2]                   = DeriveSchema.gen[CaseClassOfTuple2]
  lazy implicit val caseClassOfTuple3: Schema[CaseClassOfTuple3]                   = DeriveSchema.gen[CaseClassOfTuple3]
  lazy implicit val instantSchema: Schema.Primitive[Instant]                       =
    Schema.Primitive(StandardType.InstantType(DateTimeFormatter.ISO_INSTANT))
  lazy implicit val caseClassOfInstant: Schema[CaseClassOfInstant]                 = DeriveSchema.gen[CaseClassOfInstant]
  lazy implicit val caseClassOfStatus: Schema[CaseClassOfStatus]                   = DeriveSchema.gen[CaseClassOfStatus]

  implicit def mapSchema[String, V](implicit element: Schema[(String, V)]): Schema[Map[String, V]] = {
    val value: Schema[Chunk[(String, V)]] = chunk(element)
    value.transform(_.toMap[String, V], Chunk.fromIterable(_))
  }

  lazy implicit val caseClassOfMapOfInt: Schema[CaseClassOfMapOfInt] = DeriveSchema.gen[CaseClassOfMapOfInt]

  implicit val caseClassOfListOfTuple2: Schema[CaseClassOfListOfTuple2] = DeriveSchema.gen[CaseClassOfListOfTuple2]

  implicit val statusSchema: Schema[Status] = DeriveSchema.gen[Status]

  def toAvString(s: String): AttributeValue.String                                         = AttributeValue.String(s)
  def toAvNum(i: Int): AttributeValue.Number                                               = AttributeValue.Number(BigDecimal(i))
  def toAvList(xs: AttributeValue*): AttributeValue.List                                   = AttributeValue.List(xs.toList)
  def toAvTuple[A: ToAttributeValue, B: ToAttributeValue](a: A, b: B): AttributeValue.List =
    toAvList(ToAttributeValue[A].toAttributeValue(a), ToAttributeValue[B].toAttributeValue(b))
}
