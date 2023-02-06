package zio.dynamodb.codec

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.schema.annotation.{ caseName, discriminatorName, fieldName }
import zio.schema.{ DeriveSchema, Schema }

import java.time.Instant

// ADT example
sealed trait Status
final case class Ok(response: List[String]) extends Status
final case class Failed(code: Int, reason: String, additionalExplanation: Option[String], remark: String = "oops")
    extends Status
case object Pending                         extends Status
final case class NestedCaseClass2(id: Int, nested: SimpleCaseClass3)

final case class SimpleCaseClass3(id: Int, name: String, flag: Boolean)

final case class SimpleCaseClass3Option(id: Int, name: String, opt: Option[Int])

final case class CaseClassOfList(nums: List[Int])

final case class CaseClassOfListOfCaseClass(elements: List[SimpleCaseClass3])

final case class CaseClassOfOption(opt: Option[Int])

final case class CaseClassOfNestedOption(opt: Option[Option[Int]])

final case class CaseClassOfEither(either: Either[String, Int])

final case class CaseClassOfTuple3(tuple: (Int, Int, Int))

final case class CaseClassOfListOfTuple2(tuple: List[(String, Int)])

final case class CaseClassOfInstant(instant: Instant)

final case class CaseClassOfStatus(status: Status)

final case class CaseClassOfMapOfInt(map: Map[String, Int])

final case class CaseClassOfSetOfInt(set: Set[Int])

final case class CaseClassOfTuple2(tuple2: (String, Int))

@discriminatorName(tag = "funkyDiscriminator")
sealed trait EnumWithDiscriminator
final case class WithDiscriminatedEnum(enum: EnumWithDiscriminator)
object WithDiscriminatedEnum {
  final case class StringValue(value: String)                                 extends EnumWithDiscriminator
  final case class StringValue2(@fieldName("funky_field_name") value: String) extends EnumWithDiscriminator
  @caseName("ival")
  final case class IntValue(value: Int)                                       extends EnumWithDiscriminator
  final case object ONE                                                       extends EnumWithDiscriminator
  @caseName("2")
  final case object TWO                                                       extends EnumWithDiscriminator

  implicit val schema: Schema[WithDiscriminatedEnum] = DeriveSchema.gen[WithDiscriminatedEnum]
}

@enumOfCaseObjects // should fail runtime validation as Three is not a case object
sealed trait CaseObjectOnlyEnum2
final case class WithCaseObjectOnlyEnum2(enum: CaseObjectOnlyEnum2)
object WithCaseObjectOnlyEnum2 {
  case object ONE                                                extends CaseObjectOnlyEnum2
  @caseName("2")
  case object TWO                                                extends CaseObjectOnlyEnum2
  case class Three(@fieldName("funky_field_name") value: String) extends CaseObjectOnlyEnum2
  implicit val schema: Schema[WithCaseObjectOnlyEnum2] = DeriveSchema.gen[WithCaseObjectOnlyEnum2]
}

@enumOfCaseObjects
sealed trait CaseObjectOnlyEnum
final case class WithCaseObjectOnlyEnum(enum: CaseObjectOnlyEnum)
object WithCaseObjectOnlyEnum {
  case object ONE extends CaseObjectOnlyEnum
  @caseName("2")
  case object TWO extends CaseObjectOnlyEnum
  implicit val schema: Schema[WithCaseObjectOnlyEnum] = DeriveSchema.gen[WithCaseObjectOnlyEnum]
}

sealed trait EnumWithoutDiscriminator
final case class WithEnumWithoutDiscriminator(enum: EnumWithoutDiscriminator)
object WithEnumWithoutDiscriminator {
  @caseName("1")
  case object ONE                                                extends EnumWithoutDiscriminator
  case object TWO                                                extends EnumWithoutDiscriminator
  case class Three(@fieldName("funky_field_name") value: String) extends EnumWithoutDiscriminator
  implicit val schema: Schema[WithEnumWithoutDiscriminator] = DeriveSchema.gen[WithEnumWithoutDiscriminator]
}

@discriminatorName(tag = "funkyDiscriminator")
sealed trait Invoice {
  def id: Int
}
object Invoice       {
  final case class Billed(id: Int, i: Int)       extends Invoice
  final case class PreBilled(id: Int, s: String) extends Invoice
  implicit val schema: Schema[Invoice] = DeriveSchema.gen[Invoice]
}
