package zio.dynamodb

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.ConditionExpression.Operand.ProjectionExpressionOperand
import zio.dynamodb.ProjectionExpression.{ MapElement, Root }
import zio.random.Random
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema, Schema }
import zio.test.Assertion.{ isLeft, isRight }
import zio.test.{ assertCompletes, check, DefaultRunnableSpec, Gen, Sized }

import java.time.Instant

object KeyConditionExpressionSpec extends DefaultRunnableSpec {
  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    // report bug
    // weirdly the compiler complains about order - maybe auto derivation sorts the cases
    val schema: Schema.Enum3[CreditCard.type, DebitCard.type, PayPal.type, Payment] = DeriveSchema.gen[Payment]
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
    implicit val schema: Schema.CaseClass5[String, String, Option[Instant], Payment, List[Address], Student] =
      DeriveSchema.gen[Student]
  }

  val (email, subject, enrollmentDate, payment, addresses) = ProjectionExpression.accessors[Student]

  override def spec =
    suite("KeyConditionExpression from a ConditionExpression")(happyPathSuite, unhappyPathSuite, pbtSuite)

  val happyPathSuite   =
    suite("returns a Right for")(
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
      }
    )

  /*
   For PBT
   1) generate random no terms 1 to 4 say - `numTerms`
   2) randomly pick from ALL ops Equals, NotEquals etc `op`
   3) always use a root map element but with generated name 'fieldName'??? we should test non Root/map as well
   4) assume LHS and RHS are always string AV type
   5) for 1st term (numTerms) / or fold with 1st term as a starter
        pick an op and a field name- (fieldName, op)
        use a random string for the LHS
   6) for subsequent terms repeat above and use `&&` to create new acc value
   7) create function to check
      - check 1st op is equal
      - if left check numTerms > 2
      - if right check numTerms <= 2
   */
  sealed trait Op
  object Op {
    case object Equals    extends Op
    case object NotEquals extends Op
    val set = Set(Equals, NotEquals)
  }
  val genNumTerms: Gen[Random, Int] = Gen.int(1, 4)
  val genNames: Gen[Random with Sized, List[String]]               = genNumTerms.flatMap(i => Gen.listOfN(i)(Gen.alphaNumericString))
  val genOP                                                        = Gen.fromIterable(Op.set)
  val genNameAndOpList: Gen[Random with Sized, List[(String, Op)]] =
    Gen.listOfBounded(1, 4)(Gen.alphaNumericString zip genOP)
  // TODO - simple generator for a very limited set of PE

  def foo(name: String, op: Op): ConditionExpression =
    op match {
      case Op.Equals    =>
        ConditionExpression.Equals(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
      case Op.NotEquals =>
        ConditionExpression.NotEqual(
          ProjectionExpressionOperand(MapElement(Root, name)),
          ConditionExpression.Operand.ValueOperand(AttributeValue.String("TODO"))
        )
      // TODO:
    }

  val pbtSuite = suite("pbt suite")(testM("pbt") {
    check(genNameAndOpList) { xs =>
      val (name, op) = xs.head
      val first      = foo(name, op)
      val condEx     = xs.tail.foldRight(first) { case ((name, op), acc) => acc && foo(name, op) }
      println(condEx)
      assertCompletes
    }
  })
}
