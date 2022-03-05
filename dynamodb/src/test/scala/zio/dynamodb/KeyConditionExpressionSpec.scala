package zio.dynamodb

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.ProjectionExpression.{ MapElement, Root }
import zio.random.Random
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }
import zio.test.Assertion.{ isLeft, isRight }
import zio.test._

import java.time.Instant

object KeyConditionExpressionSpec extends DefaultRunnableSpec {
  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    val schema = DeriveSchema.gen[Payment]
  }
  final case class Address(line1: String, postcode: String)
  final case class Student(
    email: String,
    subject: String,
    enrollmentDate: Option[Instant],
    payment: Payment,
    addresses: List[Address]
  )
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema = DeriveSchema.gen[Student]
  }

  val (email, subject, enrollmentDate, payment, addresses) = ProjectionExpression.accessors[Student]

  override def spec =
    suite("KeyConditionExpression from a ConditionExpression")(happyPathSuite, unhappyPathSuite, pbtSuite)

  val happyPathSuite   =
    suite("returns a Right for")(
      test(""" email === "avi@gmail.com" """) {
        val actual = KeyConditionExpression(email === "avi@gmail.com")
        zio.test.assert(actual)(isRight)
      },
      test(""" email === "avi@gmail.com" && subject === "maths" """) {
        val actual = KeyConditionExpression(email === "avi@gmail.com" && subject === "maths")
        zio.test.assert(actual)(isRight)
      },
      test(""" email === "avi@gmail.com" && subject.beginsWith("ma") """) {
        val actual =
          KeyConditionExpression(
            email === "avi@gmail.com" && subject.beginsWith("ma")
          )
        zio.test.assert(actual)(isRight)
      }
    )
  val unhappyPathSuite =
    suite("returns a Left for")(
      test(""" email > "avi@gmail.com" && subject === "maths" """) {
        val actual = KeyConditionExpression(email > "avi@gmail.com" && subject === "maths")
        zio.test.assert(actual)(isLeft)
      },
      test(""" email >= "avi@gmail.com" && subject.beginsWith("ma") """) {
        val actual =
          KeyConditionExpression(
            email >= "avi@gmail.com" && subject.beginsWith("ma")
          )
        zio.test.assert(actual)(isLeft)
      },
      test(""" email === "avi@gmail.com" && subject.beginsWith("ma") && subject.beginsWith("ma") """) {
        val actual =
          KeyConditionExpression(
            email === "avi@gmail.com" && subject.beginsWith("ma") && subject.beginsWith("ma")
          )
        zio.test.assert(actual)(isLeft)
      }
    )

  sealed trait Op
  object Op {
    case object Equals             extends Op
    case object NotEquals          extends Op
    case object LessThan           extends Op
    case object GreaterThan        extends Op
    case object LessThanOrEqual    extends Op
    case object GreaterThanOrEqual extends Op

    val set = Set(Equals, NotEquals, LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual)
  }

  val genNumTerms: Gen[Random, Int]                                                             = Gen.int(1, 3)
  val genNames: Gen[Random with Sized, List[String]]                                            =
    genNumTerms.flatMap(i => Gen.listOfN(i)(Gen.alphaNumericStringBounded(1, 5)))
  val genOP                                                                                     = Gen.fromIterable(Op.set)
  val genNameAndOpList: Gen[Random with Sized, List[(String, Op)]]                              =
    Gen.listOfBounded(1, 4)(Gen.alphaNumericStringBounded(1, 5) zip genOP)
  val genConditionExpression: Gen[Random with Sized, (ConditionExpression, List[(String, Op)])] = genNameAndOpList.map {
    xs =>
      val (name, op) = xs.head
      val first      = conditionExpression(name, op)
      // TODO: generate joining ops
      val condEx     = xs.tail.foldRight(first) { case ((name, op), acc) => acc && conditionExpression(name, op) }
      (condEx, xs)
  }

  def conditionExpression(name: String, op: Op): ConditionExpression =
    op match {
      case Op.Equals             =>
        ConditionExpression.Equals(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
      case Op.NotEquals          =>
        ConditionExpression.NotEqual(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
      case Op.LessThan           =>
        ConditionExpression.LessThan(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
      case Op.GreaterThan        =>
        ConditionExpression.GreaterThan(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
      case Op.LessThanOrEqual    =>
        ConditionExpression.LessThanOrEqual(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
      case Op.GreaterThanOrEqual =>
        ConditionExpression.GreaterThanOrEqual(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
    }

  val pbtSuite = suite("pbt suite")(testM("pbt") {
    check(genConditionExpression) {
      case (condEx, originalList) =>
        val errorOrkeyCondExprn: Either[String, KeyConditionExpression] = KeyConditionExpression(condEx)

        assert(errorOrkeyCondExprn) {
          if (originalList.length > 2 || originalList.head._2 != Op.Equals)
            isLeft
          else
            isRight
        }
    }
  })
}
