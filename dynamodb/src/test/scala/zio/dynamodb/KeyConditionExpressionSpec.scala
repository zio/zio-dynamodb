package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.ProjectionExpression.{ MapElement, Root }
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }
import zio.test.Assertion.{ isLeft, isRight }
import zio.test._

import java.time.Instant

object KeyConditionExpressionSpec extends ZIOSpecDefault {

  final case class Student(
    email: String,
    subject: String,
    enrollmentDate: Option[Instant]
  )
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema = DeriveSchema.gen[Student]
  }

  val (email, subject, enrollmentDate) = ProjectionExpression.accessors[Student]

  override def spec =
    suite("KeyConditionExpression from a ConditionExpression")(happyPathSuite, unhappyPathSuite, pbtSuite)

  val happyPathSuite =
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

  import Generators._

  val pbtSuite =
    suite("property based suite")(test("conversion of ConditionExpression to KeyConditionExpression must be valid") {
      check(genConditionExpression) {
        case (condExprn, seedDataList) => assertConditionExpression(condExprn, seedDataList)
      }
    })

  def assertConditionExpression(
    condExprn: ConditionExpression,
    seedDataList: List[SeedData],
    printCondExprn: Boolean = false
  ): TestResult = {
    val errorOrkeyCondExprn: Either[String, KeyConditionExpression] = KeyConditionExpression(condExprn)

    if (printCondExprn) println(condExprn) else ()

    assert(errorOrkeyCondExprn) {
      if (
        seedDataList.length == 0 || seedDataList.length > maxNumOfTerms || seedDataList.head._2 != ComparisonOp.Equals
      )
        isLeft
      else
        isRight
    }
  }

  object Generators {
    sealed trait ComparisonOp
    object ComparisonOp {
      case object Equals             extends ComparisonOp
      case object NotEquals          extends ComparisonOp
      case object LessThan           extends ComparisonOp
      case object GreaterThan        extends ComparisonOp
      case object LessThanOrEqual    extends ComparisonOp
      case object GreaterThanOrEqual extends ComparisonOp

      val set = Set(Equals, NotEquals, LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual)
    }

    val maxNumOfTerms                                                   = 2
    val genOP                                                           = Gen.fromIterable(ComparisonOp.set)
    private val genFieldName: Gen[Sized, String]                        = Gen.alphaNumericStringBounded(1, 5)
    val genFieldNameAndOpList: Gen[Sized, List[(String, ComparisonOp)]] =
      Gen.listOfBounded(0, maxNumOfTerms + 1)( // ensure we generate more that max number of terms
        genFieldName zip genOP
      )
    final case class FieldNameAndComparisonOp(fieldName: String, op: ComparisonOp)
    type SeedData = (String, ComparisonOp)
    val genConditionExpression: Gen[Sized, (ConditionExpression, List[SeedData])] =
      genFieldNameAndOpList.filter(_.nonEmpty).map { xs =>
        val (name, op) = xs.head
        val first      = conditionExpression(name, op)
        // TODO: generate joining ops
        val condEx     = xs.tail.foldRight(first) { case ((name, op), acc) => acc && conditionExpression(name, op) }
        (condEx, xs)
      }

    def conditionExpression(name: String, op: ComparisonOp): ConditionExpression =
      op match {
        case ComparisonOp.Equals             =>
          ConditionExpression.Equals(
            ProjectionExpressionOperand(MapElement(Root, name)),
            ConditionExpression.Operand.ValueOperand(AttributeValue.String("SOME_VALUE"))
          )
        case ComparisonOp.NotEquals          =>
          ConditionExpression.NotEqual(
            ProjectionExpressionOperand(MapElement(Root, name)),
            ConditionExpression.Operand.ValueOperand(AttributeValue.String("SOME_VALUE"))
          )
        case ComparisonOp.LessThan           =>
          ConditionExpression.LessThan(
            ProjectionExpressionOperand(MapElement(Root, name)),
            ConditionExpression.Operand.ValueOperand(AttributeValue.String("SOME_VALUE"))
          )
        case ComparisonOp.GreaterThan        =>
          ConditionExpression.GreaterThan(
            ProjectionExpressionOperand(MapElement(Root, name)),
            ConditionExpression.Operand.ValueOperand(AttributeValue.String("SOME_VALUE"))
          )
        case ComparisonOp.LessThanOrEqual    =>
          ConditionExpression.LessThanOrEqual(
            ProjectionExpressionOperand(MapElement(Root, name)),
            ConditionExpression.Operand.ValueOperand(AttributeValue.String("SOME_VALUE"))
          )
        case ComparisonOp.GreaterThanOrEqual =>
          ConditionExpression.GreaterThanOrEqual(
            ProjectionExpressionOperand(MapElement(Root, name)),
            ConditionExpression.Operand.ValueOperand(AttributeValue.String("SOME_VALUE"))
          )
      }
  }

}
