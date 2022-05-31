package zio.dynamodb

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }
import zio.test.{ assertTrue, DefaultRunnableSpec, ZSpec }

object ProjectionExpressionSpec extends DefaultRunnableSpec {

  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    implicit val schema = DeriveSchema.gen[Payment]
  }
  final case class Student(
    email: String,
    subject: String,
    studentNumber: Int,
    collegeName: String,
    addresses: List[String] = List.empty,
    groups: Set[String] = Set.empty[String],
    payment: Payment
  )
  object Student extends DefaultJavaTimeSchemas {
    implicit val schema                                                          = DeriveSchema.gen[Student]
    val (email, subject, studentNumber, collegeName, addresses, groups, payment) =
      ProjectionExpression.accessors[Student]
  }

  override def spec: ZSpec[Environment, Failure] =
    suite("ProjectionExpressionSpec")(opSuite, functionsSuite, comparisonSuite)

  val opSuite: ZSpec[Environment, Failure] =
    suite("type safe update expressions suite")(
      test("remove") {
        val ex = Student.addresses.remove
        assertTrue(ex.toString == "RemoveAction(addresses)")
      },
      test("remove using an index") {
        val ex = Student.addresses.remove(2)
        assertTrue(ex.toString == "RemoveAction(addresses[2])")
      },
      test("set a number") {
        val ex = Student.studentNumber.set(1)
        assertTrue(ex.toString == "SetAction(studentNumber,ValueOperand(Number(1)))")
      },
      test("set using a projection expression") {
        val ex = Student.studentNumber.set(Student.studentNumber)
        assertTrue(ex.toString == "SetAction(studentNumber,PathOperand(studentNumber))")
      },
      test("set if not exists") {
        val ex = Student.studentNumber.setIfNotExists(10)
        assertTrue(ex.toString == "SetAction(studentNumber,IfNotExists(studentNumber,Number(10)))")
      },
      test("append to a list") {
        val ex = Student.groups.append("group1")
        assertTrue(ex.toString == "SetAction(groups,ListAppend(groups,List(List(String(group1)))))")
      },
      test("prepend") {
        val ex = Student.groups.prepend("group1")
        assertTrue(ex.toString == "SetAction(groups,ListPrepend(groups,List(List(String(group1)))))")
      },
      test("delete from a set") {
        val ex = Student.groups.deleteFromSet(Set("group1"))
        assertTrue(ex.toString == "DeleteAction(groups,StringSet(Set(group1)))")
      },
      test("add") {
        val ex = Student.studentNumber.add(1)
        assertTrue(ex.toString == "AddAction(studentNumber,Number(1))")
      },
      test("addSet") {
        val ex = Student.groups.addSet(Set("group2"))
        assertTrue(ex.toString == "AddAction(groups,StringSet(Set(group2)))")
      }
    )

  val functionsSuite: ZSpec[Environment, Failure] =
    suite("type safe DDB functions suite")(
      test("exists") {
        val ex = Student.email.exists
        assertTrue(ex.toString == "AttributeExists(email)")
      },
      test("notExists") {
        val ex = Student.email.notExists
        assertTrue(ex.toString == "AttributeNotExists(email)")
      },
      test("size for a set") {
        val ex = Student.groups.size
        assertTrue(ex.toString == "Size(groups)")
      },
      test("size for a list") {
        val ex = Student.addresses.size
        assertTrue(ex.toString == "Size(addresses)")
      },
      test("isBinary") {
        val ex = Student.groups.isBinary
        assertTrue(ex.toString == "AttributeType(groups,Binary)")
      },
      test("isNumber") {
        val ex = Student.groups.isNumber
        assertTrue(ex.toString == "AttributeType(groups,Number)")
      },
      test("isString") {
        val ex = Student.groups.isString
        assertTrue(ex.toString == "AttributeType(groups,String)")
      },
      test("isBool") {
        val ex = Student.groups.isBool
        assertTrue(ex.toString == "AttributeType(groups,Bool)")
      },
      test("isBinarySet") {
        val ex = Student.groups.isBinarySet
        assertTrue(ex.toString == "AttributeType(groups,BinarySet)")
      },
      test("isList") {
        val ex = Student.groups.isList
        assertTrue(ex.toString == "AttributeType(groups,List)")
      },
      test("isMap") {
        val ex = Student.groups.isMap
        assertTrue(ex.toString == "AttributeType(groups,Map)")
      },
      test("isNumberSet") {
        val ex = Student.groups.isNumberSet
        assertTrue(ex.toString == "AttributeType(groups,NumberSet)")
      },
      test("isNull") {
        val ex = Student.groups.isNull
        assertTrue(ex.toString == "AttributeType(groups,Null)")
      },
      test("isStringSet") {
        val ex = Student.groups.isStringSet
        assertTrue(ex.toString == "AttributeType(groups,StringSet)")
      },
      test("beginsWith") {
        val ex = Student.email.beginsWith("avi")
        assertTrue(ex.toString == "BeginsWith(email,String(avi))")
      },
      test("between using number range") {
        val ex = Student.studentNumber.between(1, 3)
        assertTrue(ex.toString == "Between(ProjectionExpressionOperand(studentNumber),Number(1),Number(3))")
      },
      test("inSet for a collection attribute") {
        val ex = Student.addresses.inSet(Set(List("Addr1")))
        assertTrue(ex.toString == "In(ProjectionExpressionOperand(addresses),Set(List(Chunk(String(Addr1)))))")
      },
      test("inSet for a scalar") {
        val ex = Student.studentNumber.inSet(Set(1))
        assertTrue(ex.toString == "In(ProjectionExpressionOperand(studentNumber),Set(Number(1)))")
      },
      test("'in' for a collection attribute") {
        val ex = Student.groups.in(Set("group1"), Set("group2"))
        assertTrue(
          ex.toString == "In(ProjectionExpressionOperand(groups),Set(StringSet(Set(group2)), StringSet(Set(group1))))"
        )
      },
      test("'in' for a scalar") {
        val ex = Student.studentNumber.in(1, 2)
        assertTrue(ex.toString == "In(ProjectionExpressionOperand(studentNumber),Set(Number(2), Number(1)))")
      },
      test("'in' for a sum type") {
        val ex = Student.payment.in(Payment.CreditCard, Payment.PayPal)
        assertTrue(ex.toString == "In(ProjectionExpressionOperand(payment),Set(String(PayPal), String(CreditCard)))")
      },
      test("string contains") {
        val ex = Student.collegeName.contains("foo")
        assertTrue(ex.toString == "Contains(collegeName,String(foo))")
      },
      test("set contains") {
        val ex = Student.groups.contains("group1")
        assertTrue(ex.toString == "Contains(groups,String(group1))")
      }
    )

  val comparisonSuite: ZSpec[Environment, Failure] =
    suite("type safe comparison suite")(
      test("PE === X") {
        val ex = Student.studentNumber === 1
        assertTrue(ex.toString == "Equals(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
      },
      test("PE === PE") {
        val ex = Student.studentNumber === Student.studentNumber
        assertTrue(
          ex.toString == "Equals(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
        )
      },
      test("PE <> X") {
        val ex = Student.studentNumber <> 1
        assertTrue(ex.toString == "NotEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
      },
      test("PE <> PE") {
        val ex = Student.studentNumber <> Student.studentNumber
        assertTrue(
          ex.toString == "NotEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
        )
      },
      test("PE < X") {
        val ex = Student.studentNumber < 1
        assertTrue(ex.toString == "LessThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
      },
      test("PE < PE") {
        val ex = Student.studentNumber < Student.studentNumber
        assertTrue(
          ex.toString == "LessThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
        )
      },
      test("PE <= X") {
        val ex = Student.studentNumber <= 1
        assertTrue(ex.toString == "LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
      },
      test("PE <= PE") {
        val ex = Student.studentNumber <= Student.studentNumber
        assertTrue(
          ex.toString == "LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
        )
      },
      test("PE > X") {
        val ex = Student.studentNumber > 1
        assertTrue(ex.toString == "GreaterThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
      },
      test("PE > PE") {
        val ex = Student.studentNumber > Student.studentNumber
        assertTrue(
          ex.toString == "GreaterThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
        )
      },
      test("PE >= X") {
        val ex = Student.studentNumber >= 1
        assertTrue(
          ex.toString == "GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"
        )
      },
      test("PE >= PE") {
        val ex = Student.studentNumber >= Student.studentNumber
        assertTrue(
          ex.toString == "GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
        )
      }
    )

}
