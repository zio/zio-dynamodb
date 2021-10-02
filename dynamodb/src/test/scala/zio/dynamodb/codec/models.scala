package zio.dynamodb.codec

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

final case class CaseClassOfTuple2(tuple2: (String, Int))
