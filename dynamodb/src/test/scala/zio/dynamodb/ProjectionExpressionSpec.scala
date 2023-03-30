package zio.dynamodb

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.ProjectionExpression.{ $, mapElement, MapElement, Root }
import zio.dynamodb.ExpressionAttributeNames.Prefix
import zio.schema.{ DeriveSchema, Schema }
import zio.test.Assertion._
import zio.test.{ assert, assertTrue, ZIOSpecDefault }

object ProjectionExpressionSpec extends ZIOSpecDefault {
  private val prefix = Prefix

  private val email         = s"${prefix}email"
  private val studentNumber = s"${prefix}studentNumber"
  private val collegeName   = s"${prefix}collegeName"
  private val addresses     = s"${prefix}addresses"
  private val groups        = s"${prefix}groups"
  private val payment       = s"${prefix}payment"

  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    implicit val schema: Schema.Enum3[DebitCard.type, CreditCard.type, PayPal.type, Payment] = DeriveSchema.gen[Payment]
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
  object Student {
    implicit val schema: Schema.CaseClass7[String, String, Int, String, List[String], Set[String], Payment, Student] =
      DeriveSchema.gen[Student]
    val (email, subject, studentNumber, collegeName, addresses, groups, payment)                                     =
      ProjectionExpression.accessors[Student]
  }

  override def spec =
    suite("ProjectionExpressionSpec")(
      TypeSafeSuite.typeSafeSuite,
      DollarFunctionSuite.typeUnsafeSuite,
      RawProjectionExpressionSuite.typeUnsafeSuite
    )

  object RawProjectionExpressionSuite {
    private val groupsMapEl        = mapElement(Root, "groups")
    private val addressesMapEl     = mapElement(Root, "addresses")
    private val studentNumberMapEl = mapElement(Root, "studentNumber")

    private val typeUnsafeOpSuite =
      suite("unsafe type update expressions suite")(
        test("remove") {
          val ex = addressesMapEl.remove
          assertTrue(ex.toString == s"RemoveAction($addresses)")
        },
        test("remove using an index") {
          val ex = addressesMapEl.remove(2)
          assertTrue(ex.toString == s"RemoveAction($addresses[2])")
        },
        test("set a number") {
          val ex = studentNumberMapEl.set(1)
          assertTrue(ex.toString == s"SetAction($studentNumber,ValueOperand(Number(1)))")
        },
        test("set using a raw projection expression") {
          val ex = studentNumberMapEl.set(studentNumberMapEl)
          assertTrue(ex.toString == s"SetAction($studentNumber,PathOperand($studentNumber))")
        },
        test("set using a $") {
          val ex = studentNumberMapEl.set($("studentNumber"))
          assertTrue(ex.toString == s"SetAction($studentNumber,PathOperand($studentNumber))")
        },
        test("set if not exists") {
          val ex = studentNumberMapEl.setIfNotExists(10)
          assertTrue(ex.toString == s"SetAction($studentNumber,IfNotExists($studentNumber,Number(10)))")
        },
        test("append to a list") {
          val ex = groupsMapEl.append("group1")
          assertTrue(ex.toString == s"SetAction($groups,ListAppend($groups,List(List(String(group1)))))")
        },
        test("prepend") {
          val ex = groupsMapEl.prepend("group1")
          assertTrue(ex.toString == s"SetAction($groups,ListPrepend($groups,List(List(String(group1)))))")
        },
        test("delete from a set") {
          val ex = groupsMapEl.deleteFromSet(Set("group1"))
          assertTrue(ex.toString == s"DeleteAction($groups,StringSet(Set(group1)))")
        },
        test("add") {
          val ex = studentNumberMapEl.add(1)
          assertTrue(ex.toString == s"AddAction($studentNumber,Number(1))")
        },
        test("addSet") {
          val ex = groupsMapEl.addSet(Set("group2"))
          assertTrue(ex.toString == s"AddAction($groups,StringSet(Set(group2)))")
        }
      )

    private val typeUnsafeFunctionsSuite =
      suite("type unsafe DDB functions suite")(
        test("exists") {
          val ex = MapElement(Root, "email").exists
          assertTrue(ex.toString == s"AttributeExists($email)")
        },
        test("notExists") {
          val ex = MapElement(Root, "email").notExists
          assertTrue(ex.toString == s"AttributeNotExists($email)")
        },
        test("size for a set") {
          val ex = MapElement(Root, "groups").size
          assert(ex.toString)(startsWithString(s"Size($groups,"))
        },
        test("size for a list") {
          val ex = $("addresses").size
          assert(ex.toString)(startsWithString(s"Size($addresses,"))
        },
        test("isBinary") {
          val ex = MapElement(Root, "groups").isBinary
          assertTrue(ex.toString == s"AttributeType($groups,Binary)")
        },
        test("isNumber") {
          val ex = MapElement(Root, "groups").isNumber
          assertTrue(ex.toString == s"AttributeType($groups,Number)")
        },
        test("isString") {
          val ex = MapElement(Root, "groups").isString
          assertTrue(ex.toString == s"AttributeType($groups,String)")
        },
        test("isBool") {
          val ex = MapElement(Root, "groups").isBool
          assertTrue(ex.toString == s"AttributeType($groups,Bool)")
        },
        test("isBinarySet") {
          val ex = MapElement(Root, "groups").isBinarySet
          assertTrue(ex.toString == s"AttributeType($groups,BinarySet)")
        },
        test("isList") {
          val ex = MapElement(Root, "groups").isList
          assertTrue(ex.toString == s"AttributeType($groups,List)")
        },
        test("isMap") {
          val ex = MapElement(Root, "groups").isMap
          assertTrue(ex.toString == s"AttributeType($groups,Map)")
        },
        test("isNumberSet") {
          val ex = MapElement(Root, "groups").isNumberSet
          assertTrue(ex.toString == s"AttributeType($groups,NumberSet)")
        },
        test("isNull") {
          val ex = MapElement(Root, "groups").isNull
          assertTrue(ex.toString == s"AttributeType($groups,Null)")
        },
        test("isStringSet") {
          val ex = MapElement(Root, "groups").isStringSet
          assertTrue(ex.toString == s"AttributeType($groups,StringSet)")
        },
        test("beginsWith") {
          val ex = MapElement(Root, "email").beginsWith("avi")
          assertTrue(ex.toString == s"BeginsWith($email,String(avi))")
        },
        test("between using number range") {
          val ex = MapElement(Root, "studentNumber").between(1, 3)
          assertTrue(ex.toString == s"Between(ProjectionExpressionOperand($studentNumber),Number(1),Number(3))")
        },
        test("inSet for a collection attribute") {
          val ex = MapElement(Root, "addresses").inSet(Set(List("Addr1")))
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($addresses),Set(List(Chunk(String(Addr1)))))")
        },
        test("inSet for a scalar") {
          val ex = MapElement(Root, "studentNumber").inSet(Set(1))
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($studentNumber),Set(Number(1)))")
        },
        test("'in' for a collection attribute") {
          val ex = MapElement(Root, "groups").in(Set("group1"), Set("group2"))
          assertTrue(
            ex.toString == s"In(ProjectionExpressionOperand($groups),Set(StringSet(Set(group2)), StringSet(Set(group1))))"
          )
        },
        test("'in' for a scalar") {
          val ex = MapElement(Root, "studentNumber").in(1, 2)
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($studentNumber),Set(Number(2), Number(1)))")
        },
        test("'in' for a sum type") { // note we have to use scalar values as there is no notion of sum type in the type unsafe API
          val ex = MapElement(Root, "payment").in(Payment.CreditCard.toString, Payment.PayPal.toString)
          assertTrue(
            ex.toString == s"In(ProjectionExpressionOperand($payment),Set(String(PayPal), String(CreditCard)))"
          )
        },
        test("string contains") {
          val ex = MapElement(Root, "collegeName").contains("foo")
          assertTrue(ex.toString == s"Contains($collegeName,String(foo))")
        },
        test("set contains") {
          val ex = MapElement(Root, "groups").contains("group1")
          assertTrue(ex.toString == s"Contains($groups,String(group1))")
        }
      )

    private val typeUnsafeComparisonSuite =
      suite("type safe comparison suite")(
        test("PE === X") {
          val ex = MapElement(Root, "studentNumber") === 1
          assertTrue(ex.toString == s"Equals(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE === PE") {
          val ex = $("studentNumber") === $("studentNumber")
          assertTrue(
            ex.toString == s"Equals(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE <> X") {
          val ex = $("studentNumber") <> 1
          assertTrue(ex.toString == s"NotEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE <> PE") {
          val ex = $("studentNumber") <> $("studentNumber")
          assertTrue(
            ex.toString == s"NotEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE < X") {
          val ex = $("studentNumber") < 1
          assertTrue(ex.toString == s"LessThan(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE < PE") {
          val ex = $("studentNumber") < $("studentNumber")
          assertTrue(
            ex.toString == s"LessThan(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE <= X") {
          val ex = $("studentNumber") <= 1
          assertTrue(
            ex.toString == s"LessThanOrEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))"
          )
        },
        test("PE <= PE") {
          val ex = $("studentNumber") <= $("studentNumber")
          assertTrue(
            ex.toString == s"LessThanOrEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE > X") {
          val ex = $("studentNumber") > 1
          assertTrue(ex.toString == s"GreaterThan(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE > PE") {
          val ex = $("studentNumber") > $("studentNumber")
          assertTrue(
            ex.toString == s"GreaterThan(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE >= X") {
          val ex = $("studentNumber") >= 1
          assertTrue(
            ex.toString == s"GreaterThanOrEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))"
          )
        },
        test("PE >= PE") {
          val ex = $("studentNumber") >= $("studentNumber")
          assertTrue(
            ex.toString == s"GreaterThanOrEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        }
      )

    val typeUnsafeSuite =
      suite("Raw projection expression unsafe type suite")(
        typeUnsafeOpSuite,
        typeUnsafeFunctionsSuite,
        typeUnsafeComparisonSuite
      )

  }

  object DollarFunctionSuite {
    private val typeUnsafeOpSuite =
      suite("unsafe type update expressions suite")(
        test("remove") {
          val ex = $("addresses").remove
          assertTrue(ex.toString == s"RemoveAction($addresses)")
        },
        test("remove using an index") {
          val ex = $("addresses").remove(2)
          assertTrue(ex.toString == s"RemoveAction($addresses[2])")
        },
        test("set a number") {
          val ex = $("studentNumber").set(1)
          assertTrue(ex.toString == s"SetAction($studentNumber,ValueOperand(Number(1)))")
        },
        test("set using a projection expression") {
          val ex = $("studentNumber").set(Student.studentNumber)
          assertTrue(ex.toString == s"SetAction($studentNumber,PathOperand($studentNumber))")
        },
        test("set if not exists") {
          val ex = $("studentNumber").setIfNotExists(10)
          assertTrue(ex.toString == s"SetAction($studentNumber,IfNotExists($studentNumber,Number(10)))")
        },
        test("append to a list") {
          val ex = $("groups").append("group1")
          assertTrue(ex.toString == s"SetAction($groups,ListAppend($groups,List(List(String(group1)))))")
        },
        test("prepend") {
          val ex = $("groups").prepend("group1")
          assertTrue(ex.toString == s"SetAction($groups,ListPrepend($groups,List(List(String(group1)))))")
        },
        test("delete from a set") {
          val ex = $("groups").deleteFromSet(Set("group1"))
          assertTrue(ex.toString == s"DeleteAction($groups,StringSet(Set(group1)))")
        },
        test("add") {
          val ex = $("studentNumber").add(1)
          assertTrue(ex.toString == s"AddAction($studentNumber,Number(1))")
        },
        test("addSet") {
          val ex = $("groups").addSet(Set("group2"))
          assertTrue(ex.toString == s"AddAction($groups,StringSet(Set(group2)))")
        }
      )

    private val typeUnsafeFunctionsSuite =
      suite("type unsafe DDB functions suite")(
        test("exists") {
          val ex = $("email").exists
          assertTrue(ex.toString == s"AttributeExists($email)")
        },
        test("notExists") {
          val ex = $("email").notExists
          assertTrue(ex.toString == s"AttributeNotExists($email)")
        },
        test("size for a set") {
          val ex = $("groups").size
          assert(ex.toString)(startsWithString(s"Size($groups,"))
        },
        test("size for a list") {
          val ex = $("addresses").size
          assert(ex.toString)(startsWithString(s"Size($addresses,"))
        },
        test("isBinary") {
          val ex = $("groups").isBinary
          assertTrue(ex.toString == s"AttributeType($groups,Binary)")
        },
        test("isNumber") {
          val ex = $("groups").isNumber
          assertTrue(ex.toString == s"AttributeType($groups,Number)")
        },
        test("isString") {
          val ex = $("groups").isString
          assertTrue(ex.toString == s"AttributeType($groups,String)")
        },
        test("isBool") {
          val ex = $("groups").isBool
          assertTrue(ex.toString == s"AttributeType($groups,Bool)")
        },
        test("isBinarySet") {
          val ex = $("groups").isBinarySet
          assertTrue(ex.toString == s"AttributeType($groups,BinarySet)")
        },
        test("isList") {
          val ex = $("groups").isList
          assertTrue(ex.toString == s"AttributeType($groups,List)")
        },
        test("isMap") {
          val ex = $("groups").isMap
          assertTrue(ex.toString == s"AttributeType($groups,Map)")
        },
        test("isNumberSet") {
          val ex = $("groups").isNumberSet
          assertTrue(ex.toString == s"AttributeType($groups,NumberSet)")
        },
        test("isNull") {
          val ex = $("groups").isNull
          assertTrue(ex.toString == s"AttributeType($groups,Null)")
        },
        test("isStringSet") {
          val ex = $("groups").isStringSet
          assertTrue(ex.toString == s"AttributeType($groups,StringSet)")
        },
        test("beginsWith") {
          val ex = $("email").beginsWith("avi")
          assertTrue(ex.toString == s"BeginsWith($email,String(avi))")
        },
        test("between using number range") {
          val ex = $("studentNumber").between(1, 3)
          assertTrue(ex.toString == s"Between(ProjectionExpressionOperand($studentNumber),Number(1),Number(3))")
        },
        test("inSet for a collection attribute") {
          val ex = $("addresses").inSet(Set(List("Addr1")))
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($addresses),Set(List(Chunk(String(Addr1)))))")
        },
        test("inSet for a scalar") {
          val ex = $("studentNumber").inSet(Set(1))
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($studentNumber),Set(Number(1)))")
        },
        test("'in' for a collection attribute") {
          val ex = $("groups").in(Set("group1"), Set("group2"))
          assertTrue(
            ex.toString == s"In(ProjectionExpressionOperand($groups),Set(StringSet(Set(group2)), StringSet(Set(group1))))"
          )
        },
        test("'in' for a scalar") {
          val ex = $("studentNumber").in(1, 2)
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($studentNumber),Set(Number(2), Number(1)))")
        },
        test("'in' for a sum type") { // note we have to use scalar values as there is no notion of sum type in the type unsafe API
          val ex = $("payment").in(Payment.CreditCard.toString, Payment.PayPal.toString)
          assertTrue(
            ex.toString == s"In(ProjectionExpressionOperand($payment),Set(String(PayPal), String(CreditCard)))"
          )
        },
        test("string contains") {
          val ex = $("collegeName").contains("foo")
          assertTrue(ex.toString == s"Contains($collegeName,String(foo))")
        },
        test("set contains") {
          val ex = $("groups").contains("group1")
          assertTrue(ex.toString == s"Contains($groups,String(group1))")
        }
      )

    private val typeUnsafeComparisonSuite =
      suite("type safe comparison suite")(
        test("PE === X") {
          val ex = $("studentNumber") === 1
          assertTrue(ex.toString == s"Equals(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE === PE") {
          val ex = $("studentNumber") === $("studentNumber")
          assertTrue(
            ex.toString == s"Equals(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE <> X") {
          val ex = $("studentNumber") <> 1
          assertTrue(ex.toString == s"NotEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE <> PE") {
          val ex = $("studentNumber") <> $("studentNumber")
          assertTrue(
            ex.toString == s"NotEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE < X") {
          val ex = $("studentNumber") < 1
          assertTrue(ex.toString == s"LessThan(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE < PE") {
          val ex = $("studentNumber") < $("studentNumber")
          assertTrue(
            ex.toString == s"LessThan(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE <= X") {
          val ex = $("studentNumber") <= 1
          assertTrue(
            ex.toString == s"LessThanOrEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))"
          )
        },
        test("PE <= PE") {
          val ex = $("studentNumber") <= $("studentNumber")
          assertTrue(
            ex.toString == s"LessThanOrEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE > X") {
          val ex = $("studentNumber") > 1
          assertTrue(ex.toString == s"GreaterThan(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE > PE") {
          val ex = $("studentNumber") > $("studentNumber")
          assertTrue(
            ex.toString == s"GreaterThan(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE >= X") {
          val ex = $("studentNumber") >= 1
          assertTrue(
            ex.toString == s"GreaterThanOrEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))"
          )
        },
        test("PE >= PE") {
          val ex = $("studentNumber") >= $("studentNumber")
          assertTrue(
            ex.toString == s"GreaterThanOrEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        }
      )

    val typeUnsafeSuite =
      suite("Dollar function unsafe type suite")(
        typeUnsafeOpSuite,
        typeUnsafeFunctionsSuite,
        typeUnsafeComparisonSuite
      )
  }

  object TypeSafeSuite {
    private val typeSafeOpSuite =
      suite("type safe update expressions suite")(
        test("remove") {
          val ex = Student.addresses.remove
          assertTrue(ex.toString == s"RemoveAction($addresses)")
        },
        test("remove using an index") {
          val ex = Student.addresses.remove(2)
          assertTrue(ex.toString == s"RemoveAction($addresses[2])")
        },
        test("set a number") {
          val ex = Student.studentNumber.set(1)
          assertTrue(ex.toString == s"SetAction($studentNumber,ValueOperand(Number(1)))")
        },
        test("set using a projection expression") {
          val ex = Student.studentNumber.set(Student.studentNumber)
          assertTrue(ex.toString == s"SetAction($studentNumber,PathOperand($studentNumber))")
        },
        test("set if not exists") {
          val ex = Student.studentNumber.setIfNotExists(10)
          assertTrue(ex.toString == s"SetAction($studentNumber,IfNotExists($studentNumber,Number(10)))")
        },
        test("append to a list") {
          val ex = Student.groups.append("group1")
          assertTrue(ex.toString == s"SetAction($groups,ListAppend($groups,List(List(String(group1)))))")
        },
        test("prepend") {
          val ex = Student.groups.prepend("group1")
          assertTrue(ex.toString == s"SetAction($groups,ListPrepend($groups,List(List(String(group1)))))")
        },
        test("delete from a set") {
          val ex = Student.groups.deleteFromSet(Set("group1"))
          assertTrue(ex.toString == s"DeleteAction($groups,StringSet(Set(group1)))")
        },
        test("add") {
          val ex = Student.studentNumber.add(1)
          assertTrue(ex.toString == s"AddAction($studentNumber,Number(1))")
        },
        test("addSet") {
          val ex = Student.groups.addSet(Set("group2"))
          assertTrue(ex.toString == s"AddAction($groups,StringSet(Set(group2)))")
        }
      )

    private val typeSafeFunctionsSuite =
      suite("type safe DDB functions suite")(
        test("exists") {
          val ex = Student.email.exists
          assertTrue(ex.toString == s"AttributeExists($email)")
        },
        test("notExists") {
          val ex = Student.email.notExists
          assertTrue(ex.toString == s"AttributeNotExists($email)")
        },
        test("size for a set") {
          val ex = Student.groups.size
          assert(ex.toString)(startsWithString(s"Size($groups,"))
        },
        test("size for a list") {
          val ex = Student.addresses.size
          assert(ex.toString)(startsWithString(s"Size($addresses,"))
        },
        test("isBinary") {
          val ex = Student.groups.isBinary
          assertTrue(ex.toString == s"AttributeType($groups,Binary)")
        },
        test("isNumber") {
          val ex = Student.groups.isNumber
          assertTrue(ex.toString == s"AttributeType($groups,Number)")
        },
        test("isString") {
          val ex = Student.groups.isString
          assertTrue(ex.toString == s"AttributeType($groups,String)")
        },
        test("isBool") {
          val ex = Student.groups.isBool
          assertTrue(ex.toString == s"AttributeType($groups,Bool)")
        },
        test("isBinarySet") {
          val ex = Student.groups.isBinarySet
          assertTrue(ex.toString == s"AttributeType($groups,BinarySet)")
        },
        test("isList") {
          val ex = Student.groups.isList
          assertTrue(ex.toString == s"AttributeType($groups,List)")
        },
        test("isMap") {
          val ex = Student.groups.isMap
          assertTrue(ex.toString == s"AttributeType($groups,Map)")
        },
        test("isNumberSet") {
          val ex = Student.groups.isNumberSet
          assertTrue(ex.toString == s"AttributeType($groups,NumberSet)")
        },
        test("isNull") {
          val ex = Student.groups.isNull
          assertTrue(ex.toString == s"AttributeType($groups,Null)")
        },
        test("isStringSet") {
          val ex = Student.groups.isStringSet
          assertTrue(ex.toString == s"AttributeType($groups,StringSet)")
        },
        test("beginsWith") {
          val ex = Student.email.beginsWith("avi")
          assertTrue(ex.toString == s"BeginsWith($email,String(avi))")
        },
        test("between using number range") {
          val ex = Student.studentNumber.between(1, 3)
          assertTrue(ex.toString == s"Between(ProjectionExpressionOperand($studentNumber),Number(1),Number(3))")
        },
        test("inSet for a collection attribute") {
          val ex = Student.addresses.inSet(Set(List("Addr1")))
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($addresses),Set(List(Chunk(String(Addr1)))))")
        },
        test("inSet for a scalar") {
          val ex = Student.studentNumber.inSet(Set(1))
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($studentNumber),Set(Number(1)))")
        },
        test("'in' for a collection attribute") {
          val ex = Student.groups.in(Set("group1"), Set("group2"))
          assertTrue(
            ex.toString == s"In(ProjectionExpressionOperand($groups),Set(StringSet(Set(group2)), StringSet(Set(group1))))"
          )
        },
        test("'in' for a scalar") {
          val ex = Student.studentNumber.in(1, 2)
          assertTrue(ex.toString == s"In(ProjectionExpressionOperand($studentNumber),Set(Number(2), Number(1)))")
        },
        test("'in' for a sum type") {
          val ex = Student.payment.in(Payment.CreditCard, Payment.PayPal)
          assertTrue(
            ex.toString == s"In(ProjectionExpressionOperand($payment),Set(String(PayPal), String(CreditCard)))"
          )
        },
        test("string contains") {
          val ex = Student.collegeName.contains("foo")
          assertTrue(ex.toString == s"Contains($collegeName,String(foo))")
        },
        test("set contains") {
          val ex = Student.groups.contains("group1")
          assertTrue(ex.toString == s"Contains($groups,String(group1))")
        }
      )

    private val typeSafeComparisonSuite =
      suite("type safe comparison suite")(
        test("PE === X") {
          val ex = Student.studentNumber === 1
          assertTrue(ex.toString == s"Equals(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE === PE") {
          val ex = Student.studentNumber === Student.studentNumber
          assertTrue(
            ex.toString == s"Equals(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE <> X") {
          val ex = Student.studentNumber <> 1
          assertTrue(ex.toString == s"NotEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE <> PE") {
          val ex = Student.studentNumber <> Student.studentNumber
          assertTrue(
            ex.toString == s"NotEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE < X") {
          val ex = Student.studentNumber < 1
          assertTrue(ex.toString == s"LessThan(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE < PE") {
          val ex = Student.studentNumber < Student.studentNumber
          assertTrue(
            ex.toString == s"LessThan(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE <= X") {
          val ex = Student.studentNumber <= 1
          assertTrue(
            ex.toString == s"LessThanOrEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))"
          )
        },
        test("PE <= PE") {
          val ex = Student.studentNumber <= Student.studentNumber
          assertTrue(
            ex.toString == s"LessThanOrEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE > X") {
          val ex = Student.studentNumber > 1
          assertTrue(ex.toString == s"GreaterThan(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))")
        },
        test("PE > PE") {
          val ex = Student.studentNumber > Student.studentNumber
          assertTrue(
            ex.toString == s"GreaterThan(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        },
        test("PE >= X") {
          val ex = Student.studentNumber >= 1
          assertTrue(
            ex.toString == s"GreaterThanOrEqual(ProjectionExpressionOperand($studentNumber),ValueOperand(Number(1)))"
          )
        },
        test("PE >= PE") {
          val ex = Student.studentNumber >= Student.studentNumber
          assertTrue(
            ex.toString == s"GreaterThanOrEqual(ProjectionExpressionOperand($studentNumber),ProjectionExpressionOperand($studentNumber))"
          )
        }
      )

    val typeSafeSuite =
      suite("type safe suite")(
        typeSafeOpSuite,
        typeSafeFunctionsSuite,
        typeSafeComparisonSuite
      )
  }

}
