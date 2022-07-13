//package zio.dynamodb
//
//import zio.dynamodb.Annotations.enumOfCaseObjects
//import zio.dynamodb.ProjectionExpression.{ $, mapElement, MapElement, Root }
//import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema }
//import zio.test.Assertion.equalTo
//import zio.test.{ assert, ZIOSpecDefault }
//
//object ProjectionExpressionSpec2 extends ZIOSpecDefault {
//  @enumOfCaseObjects
//  sealed trait Payment
//  object Payment {
//    final case object DebitCard  extends Payment
//    final case object CreditCard extends Payment
//    final case object PayPal     extends Payment
//
//    implicit val schema = DeriveSchema.gen[Payment]
//  }
//  final case class Student(
//    email: String,
//    subject: String,
//    studentNumber: Int,
//    collegeName: String,
//    addresses: List[String] = List.empty,
//    groups: Set[String] = Set.empty[String],
//    payment: Payment
//  )
//  object Student extends DefaultJavaTimeSchemas {
//    implicit val schema                                                          = DeriveSchema.gen[Student]
//    val (email, subject, studentNumber, collegeName, addresses, groups, payment) =
//      ProjectionExpression.accessors[Student](schema)
//  }
//
//  override def spec =
//    suite("ProjectionExpressionSpec")(
//      TypeSafeSuite.typeSafeSuite,
//      DollarFunctionSuite.typeUnsafeSuite,
//      RawProjectionExpressionSuite.typeUnsafeSuite
//    )
//
//  object RawProjectionExpressionSuite {
//    private val groups        = mapElement(Root, "groups")
//    private val addresses     = mapElement(Root, "addresses")
//    private val studentNumber = mapElement(Root, "studentNumber")
//
//    private val typeUnsafeOpSuite =
//      suite("unsafe type update expressions suite")(
//        test("remove") {
//          val ex = addresses.remove
//          assert(ex.toString)(equalTo("RemoveAction(addresses)"))
//        },
//        test("remove using an index") {
//          val ex = addresses.remove(2)
//          assert(ex.toString)(equalTo("RemoveAction(addresses[2])"))
//        },
//        test("set a number") {
//          val ex = studentNumber.set(1)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,ValueOperand(Number(1)))"))
//        },
//        test("set using a raw projection expression") {
//          val ex = studentNumber.set(studentNumber)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,PathOperand(studentNumber))"))
//        },
//        test("set using a $") {
//          val ex = studentNumber.set($("studentNumber"))
//          assert(ex.toString)(equalTo("SetAction(studentNumber,PathOperand(studentNumber))"))
//        },
//        test("set if not exists") {
//          val ex = studentNumber.setIfNotExists(10)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,IfNotExists(studentNumber,Number(10)))"))
//        },
//        test("append to a list") {
//          val ex = groups.append("group1")
//          assert(ex.toString)(equalTo("SetAction(groups,ListAppend(groups,List(List(String(group1)))))"))
//        },
//        test("prepend") {
//          val ex = groups.prepend("group1")
//          assert(ex.toString)(equalTo("SetAction(groups,ListPrepend(groups,List(List(String(group1)))))"))
//        },
//        test("delete from a set") {
//          val ex = groups.deleteFromSet(Set("group1"))
//          assert(ex.toString)(equalTo("DeleteAction(groups,StringSet(Set(group1)))"))
//        },
//        test("add") {
//          val ex = studentNumber.add(1)
//          assert(ex.toString)(equalTo("AddAction(studentNumber,Number(1))"))
//        },
//        test("addSet") {
//          val ex = groups.addSet(Set("group2"))
//          assert(ex.toString)(equalTo("AddAction(groups,StringSet(Set(group2)))"))
//        }
//      )
//
//    private val typeUnsafeFunctionsSuite =
//      suite("type unsafe DDB functions suite")(
//        test("exists") {
//          val ex = MapElement(Root, "email").exists
//          assert(ex.toString)(equalTo("AttributeExists(email)"))
//        },
//        test("notExists") {
//          val ex = MapElement(Root, "email").notExists
//          assert(ex.toString)(equalTo("AttributeNotExists(email)"))
//        },
//        test("size for a set") {
//          val ex = MapElement(Root, "groups").size
//          assert(ex.toString)(equalTo("Size(groups)"))
//        },
//        test("size for a list") {
//          val ex = $("addresses").size
//          assert(ex.toString)(equalTo("Size(addresses)"))
//        },
//        test("isBinary") {
//          val ex = MapElement(Root, "groups").isBinary
//          assert(ex.toString)(equalTo("AttributeType(groups,Binary)"))
//        },
//        test("isNumber") {
//          val ex = MapElement(Root, "groups").isNumber
//          assert(ex.toString)(equalTo("AttributeType(groups,Number)"))
//        },
//        test("isString") {
//          val ex = MapElement(Root, "groups").isString
//          assert(ex.toString)(equalTo("AttributeType(groups,String)"))
//        },
//        test("isBool") {
//          val ex = MapElement(Root, "groups").isBool
//          assert(ex.toString)(equalTo("AttributeType(groups,Bool)"))
//        },
//        test("isBinarySet") {
//          val ex = MapElement(Root, "groups").isBinarySet
//          assert(ex.toString)(equalTo("AttributeType(groups,BinarySet)"))
//        },
//        test("isList") {
//          val ex = MapElement(Root, "groups").isList
//          assert(ex.toString)(equalTo("AttributeType(groups,List)"))
//        },
//        test("isMap") {
//          val ex = MapElement(Root, "groups").isMap
//          assert(ex.toString)(equalTo("AttributeType(groups,Map)"))
//        },
//        test("isNumberSet") {
//          val ex = MapElement(Root, "groups").isNumberSet
//          assert(ex.toString)(equalTo("AttributeType(groups,NumberSet)"))
//        },
//        test("isNull") {
//          val ex = MapElement(Root, "groups").isNull
//          assert(ex.toString)(equalTo("AttributeType(groups,Null)"))
//        },
//        test("isStringSet") {
//          val ex = MapElement(Root, "groups").isStringSet
//          assert(ex.toString)(equalTo("AttributeType(groups,StringSet)"))
//        },
//        test("beginsWith") {
//          val ex = MapElement(Root, "email").beginsWith("avi")
//          assert(ex.toString)(equalTo("BeginsWith(email,String(avi))"))
//        },
//        test("between using number range") {
//          val ex = MapElement(Root, "studentNumber").between(1, 3)
//          assert(ex.toString)(equalTo("Between(ProjectionExpressionOperand(studentNumber),Number(1),Number(3))"))
//        },
//        test("inSet for a collection attribute") {
//          val ex = MapElement(Root, "addresses").inSet(Set(List("Addr1")))
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(addresses),Set(List(Chunk(String(Addr1)))))"))
//        },
//        test("inSet for a scalar") {
//          val ex = MapElement(Root, "studentNumber").inSet(Set(1))
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(studentNumber),Set(Number(1)))"))
//        },
//        test("'in' for a collection attribute") {
//          val ex = MapElement(Root, "groups").in(Set("group1"), Set("group2"))
//          assert(ex.toString)(
//            equalTo("In(ProjectionExpressionOperand(groups),Set(StringSet(Set(group2)), StringSet(Set(group1))))")
//          )
//        },
//        test("'in' for a scalar") {
//          val ex = MapElement(Root, "studentNumber").in(1, 2)
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(studentNumber),Set(Number(2), Number(1)))"))
//        },
//        test("'in' for a sum type") { // note we have to use scalar values as there is no notion of sum type in the type unsafe API
//          val ex = MapElement(Root, "payment").in(Payment.CreditCard.toString, Payment.PayPal.toString)
//          assert(ex.toString)(
//            equalTo("In(ProjectionExpressionOperand(payment),Set(String(PayPal), String(CreditCard)))")
//          )
//        },
//        test("string contains") {
//          val ex = MapElement(Root, "collegeName").contains("foo")
//          assert(ex.toString)(equalTo("Contains(collegeName,String(foo))"))
//        },
//        test("set contains") {
//          val ex = MapElement(Root, "groups").contains("group1")
//          assert(ex.toString)(equalTo("Contains(groups,String(group1))"))
//        }
//      )
//
//    private val typeUnsafeComparisonSuite =
//      suite("type safe comparison suite")(
//        test("PE === X") {
//          val ex = MapElement(Root, "studentNumber") === 1
//          assert(ex.toString)(equalTo("Equals(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE === PE") {
//          val ex = $("studentNumber") === $("studentNumber")
//          assert(ex.toString)(
//            equalTo("Equals(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE <> X") {
//          val ex = $("studentNumber") <> 1
//          assert(ex.toString)(equalTo("NotEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE <> PE") {
//          val ex = $("studentNumber") <> $("studentNumber")
//          assert(ex.toString)(
//            equalTo("NotEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE < X") {
//          val ex = $("studentNumber") < 1
//          assert(ex.toString)(equalTo("LessThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE < PE") {
//          val ex = $("studentNumber") < $("studentNumber")
//          assert(ex.toString)(
//            equalTo("LessThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE <= X") {
//          val ex = $("studentNumber") <= 1
//          assert(ex.toString)(
//            equalTo("LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE <= PE") {
//          val ex = $("studentNumber") <= $("studentNumber")
//          assert(ex.toString)(
//            equalTo(
//              "LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        },
//        test("PE > X") {
//          val ex = $("studentNumber") > 1
//          assert(ex.toString)(
//            equalTo("GreaterThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE > PE") {
//          val ex = $("studentNumber") > $("studentNumber")
//          assert(ex.toString)(
//            equalTo(
//              "GreaterThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        },
//        test("PE >= X") {
//          val ex = $("studentNumber") >= 1
//          assert(ex.toString)(
//            equalTo("GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE >= PE") {
//          val ex = $("studentNumber") >= $("studentNumber")
//          assert(ex.toString)(
//            equalTo(
//              "GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        }
//      )
//
//    val typeUnsafeSuite =
//      suite("Raw projection expression unsafe type suite")(
//        typeUnsafeOpSuite,
//        typeUnsafeFunctionsSuite,
//        typeUnsafeComparisonSuite
//      )
//
//  }
//
//  object DollarFunctionSuite {
//    private val typeUnsafeOpSuite =
//      suite("unsafe type update expressions suite")(
//        test("remove") {
//          val ex = $("addresses").remove
//          assert(ex.toString)(equalTo("RemoveAction(addresses)"))
//        },
//        test("remove using an index") {
//          val ex = $("addresses").remove(2)
//          assert(ex.toString)(equalTo("RemoveAction(addresses[2])"))
//        },
//        test("set a number") {
//          val ex = $("studentNumber").set(1)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,ValueOperand(Number(1)))"))
//        },
//        test("set using a projection expression") {
//          val ex = $("studentNumber").set(Student.studentNumber)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,PathOperand(studentNumber))"))
//        },
//        test("set if not exists") {
//          val ex = $("studentNumber").setIfNotExists(10)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,IfNotExists(studentNumber,Number(10)))"))
//        },
//        test("append to a list") {
//          val ex = $("groups").append("group1")
//          assert(ex.toString)(equalTo("SetAction(groups,ListAppend(groups,List(List(String(group1)))))"))
//        },
//        test("prepend") {
//          val ex = $("groups").prepend("group1")
//          assert(ex.toString)(equalTo("SetAction(groups,ListPrepend(groups,List(List(String(group1)))))"))
//        },
//        test("delete from a set") {
//          val ex = $("groups").deleteFromSet(Set("group1"))
//          assert(ex.toString)(equalTo("DeleteAction(groups,StringSet(Set(group1)))"))
//        },
//        test("add") {
//          val ex = $("studentNumber").add(1)
//          assert(ex.toString)(equalTo("AddAction(studentNumber,Number(1))"))
//        },
//        test("addSet") {
//          val ex = $("groups").addSet(Set("group2"))
//          assert(ex.toString)(equalTo("AddAction(groups,StringSet(Set(group2)))"))
//        }
//      )
//
//    private val typeUnsafeFunctionsSuite =
//      suite("type unsafe DDB functions suite")(
//        test("exists") {
//          val ex = $("email").exists
//          assert(ex.toString)(equalTo("AttributeExists(email)"))
//        },
//        test("notExists") {
//          val ex = $("email").notExists
//          assert(ex.toString)(equalTo("AttributeNotExists(email)"))
//        },
//        test("size for a set") {
//          val ex = $("groups").size
//          assert(ex.toString)(equalTo("Size(groups)"))
//        },
//        test("size for a list") {
//          val ex = $("addresses").size
//          assert(ex.toString)(equalTo("Size(addresses)"))
//        },
//        test("isBinary") {
//          val ex = $("groups").isBinary
//          assert(ex.toString)(equalTo("AttributeType(groups,Binary)"))
//        },
//        test("isNumber") {
//          val ex = $("groups").isNumber
//          assert(ex.toString)(equalTo("AttributeType(groups,Number)"))
//        },
//        test("isString") {
//          val ex = $("groups").isString
//          assert(ex.toString)(equalTo("AttributeType(groups,String)"))
//        },
//        test("isBool") {
//          val ex = $("groups").isBool
//          assert(ex.toString)(equalTo("AttributeType(groups,Bool)"))
//        },
//        test("isBinarySet") {
//          val ex = $("groups").isBinarySet
//          assert(ex.toString)(equalTo("AttributeType(groups,BinarySet)"))
//        },
//        test("isList") {
//          val ex = $("groups").isList
//          assert(ex.toString)(equalTo("AttributeType(groups,List)"))
//        },
//        test("isMap") {
//          val ex = $("groups").isMap
//          assert(ex.toString)(equalTo("AttributeType(groups,Map)"))
//        },
//        test("isNumberSet") {
//          val ex = $("groups").isNumberSet
//          assert(ex.toString)(equalTo("AttributeType(groups,NumberSet)"))
//        },
//        test("isNull") {
//          val ex = $("groups").isNull
//          assert(ex.toString)(equalTo("AttributeType(groups,Null)"))
//        },
//        test("isStringSet") {
//          val ex = $("groups").isStringSet
//          assert(ex.toString)(equalTo("AttributeType(groups,StringSet)"))
//        },
//        test("beginsWith") {
//          val ex = $("email").beginsWith("avi")
//          assert(ex.toString)(equalTo("BeginsWith(email,String(avi))"))
//        },
//        test("between using number range") {
//          val ex = $("studentNumber").between(1, 3)
//          assert(ex.toString)(equalTo("Between(ProjectionExpressionOperand(studentNumber),Number(1),Number(3))"))
//        },
//        test("inSet for a collection attribute") {
//          val ex = $("addresses").inSet(Set(List("Addr1")))
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(addresses),Set(List(Chunk(String(Addr1)))))"))
//        },
//        test("inSet for a scalar") {
//          val ex = $("studentNumber").inSet(Set(1))
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(studentNumber),Set(Number(1)))"))
//        },
//        test("'in' for a collection attribute") {
//          val ex = $("groups").in(Set("group1"), Set("group2"))
//          assert(ex.toString)(
//            equalTo("In(ProjectionExpressionOperand(groups),Set(StringSet(Set(group2)), StringSet(Set(group1))))")
//          )
//        },
//        test("'in' for a scalar") {
//          val ex = $("studentNumber").in(1, 2)
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(studentNumber),Set(Number(2), Number(1)))"))
//        },
//        test("'in' for a sum type") { // note we have to use scalar values as there is no notion of sum type in the type unsafe API
//          val ex = $("payment").in(Payment.CreditCard.toString, Payment.PayPal.toString)
//          assert(ex.toString)(
//            equalTo("In(ProjectionExpressionOperand(payment),Set(String(PayPal), String(CreditCard)))")
//          )
//        },
//        test("string contains") {
//          val ex = $("collegeName").contains("foo")
//          assert(ex.toString)(equalTo("Contains(collegeName,String(foo))"))
//        },
//        test("set contains") {
//          val ex = $("groups").contains("group1")
//          assert(ex.toString)(equalTo("Contains(groups,String(group1))"))
//        }
//      )
//
//    private val typeUnsafeComparisonSuite =
//      suite("type safe comparison suite")(
//        test("PE === X") {
//          val ex = $("studentNumber") === 1
//          assert(ex.toString)(equalTo("Equals(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE === PE") {
//          val ex = $("studentNumber") === $("studentNumber")
//          assert(ex.toString)(
//            equalTo("Equals(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE <> X") {
//          val ex = $("studentNumber") <> 1
//          assert(ex.toString)(equalTo("NotEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE <> PE") {
//          val ex = $("studentNumber") <> $("studentNumber")
//          assert(ex.toString)(
//            equalTo("NotEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE < X") {
//          val ex = $("studentNumber") < 1
//          assert(ex.toString)(equalTo("LessThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE < PE") {
//          val ex = $("studentNumber") < $("studentNumber")
//          assert(ex.toString)(
//            equalTo("LessThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE <= X") {
//          val ex = $("studentNumber") <= 1
//          assert(ex.toString)(
//            equalTo("LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE <= PE") {
//          val ex = $("studentNumber") <= $("studentNumber")
//          assert(ex.toString)(
//            equalTo(
//              "LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        },
//        test("PE > X") {
//          val ex = $("studentNumber") > 1
//          assert(ex.toString)(
//            equalTo("GreaterThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE > PE") {
//          val ex = $("studentNumber") > $("studentNumber")
//          assert(ex.toString)(
//            equalTo(
//              "GreaterThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        },
//        test("PE >= X") {
//          val ex = $("studentNumber") >= 1
//          assert(ex.toString)(
//            equalTo("GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE >= PE") {
//          val ex = $("studentNumber") >= $("studentNumber")
//          assert(ex.toString)(
//            equalTo(
//              "GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        }
//      )
//
//    val typeUnsafeSuite =
//      suite("Dollar function unsafe type suite")(
//        typeUnsafeOpSuite,
//        typeUnsafeFunctionsSuite,
//        typeUnsafeComparisonSuite
//      )
//  }
//
//  object TypeSafeSuite {
//    private val typeSafeOpSuite =
//      suite("type safe update expressions suite")(
//        test("remove") {
//          val ex = Student.addresses.remove
//          assert(ex.toString)(equalTo("RemoveAction(addresses)"))
//        },
//        test("remove using an index") {
//          val ex = Student.addresses.remove(2)
//          assert(ex.toString)(equalTo("RemoveAction(addresses[2])"))
//        },
//        test("set a number") {
//          val ex = Student.studentNumber.set(1)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,ValueOperand(Number(1)))"))
//        },
//        test("set using a projection expression") {
//          val ex = Student.studentNumber.set(Student.studentNumber)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,PathOperand(studentNumber))"))
//        },
//        test("set if not exists") {
//          val ex = Student.studentNumber.setIfNotExists(10)
//          assert(ex.toString)(equalTo("SetAction(studentNumber,IfNotExists(studentNumber,Number(10)))"))
//        },
//        test("append to a list") {
//          val ex = Student.groups.append("group1")
//          assert(ex.toString)(equalTo("SetAction(groups,ListAppend(groups,List(List(String(group1)))))"))
//        },
//        test("prepend") {
//          val ex = Student.groups.prepend("group1")
//          assert(ex.toString)(equalTo("SetAction(groups,ListPrepend(groups,List(List(String(group1)))))"))
//        },
//        test("delete from a set") {
//          val ex = Student.groups.deleteFromSet(Set("group1"))
//          assert(ex.toString)(equalTo("DeleteAction(groups,StringSet(Set(group1)))"))
//        },
//        test("add") {
//          val ex = Student.studentNumber.add(1)
//          assert(ex.toString)(equalTo("AddAction(studentNumber,Number(1))"))
//        },
//        test("addSet") {
//          val ex = Student.groups.addSet(Set("group2"))
//          assert(ex.toString)(equalTo("AddAction(groups,StringSet(Set(group2)))"))
//        }
//      )
//
//    private val typeSafeFunctionsSuite =
//      suite("type safe DDB functions suite")(
//        test("exists") {
//          val ex = Student.email.exists
//          assert(ex.toString)(equalTo("AttributeExists(email)"))
//        },
//        test("notExists") {
//          val ex = Student.email.notExists
//          assert(ex.toString)(equalTo("AttributeNotExists(email)"))
//        },
//        test("size for a set") {
//          val ex = Student.groups.size
//          assert(ex.toString)(equalTo("Size(groups)"))
//        },
//        test("size for a list") {
//          val ex = Student.addresses.size
//          assert(ex.toString)(equalTo("Size(addresses)"))
//        },
//        test("isBinary") {
//          val ex = Student.groups.isBinary
//          assert(ex.toString)(equalTo("AttributeType(groups,Binary)"))
//        },
//        test("isNumber") {
//          val ex = Student.groups.isNumber
//          assert(ex.toString)(equalTo("AttributeType(groups,Number)"))
//        },
//        test("isString") {
//          val ex = Student.groups.isString
//          assert(ex.toString)(equalTo("AttributeType(groups,String)"))
//        },
//        test("isBool") {
//          val ex = Student.groups.isBool
//          assert(ex.toString)(equalTo("AttributeType(groups,Bool)"))
//        },
//        test("isBinarySet") {
//          val ex = Student.groups.isBinarySet
//          assert(ex.toString)(equalTo("AttributeType(groups,BinarySet)"))
//        },
//        test("isList") {
//          val ex = Student.groups.isList
//          assert(ex.toString)(equalTo("AttributeType(groups,List)"))
//        },
//        test("isMap") {
//          val ex = Student.groups.isMap
//          assert(ex.toString)(equalTo("AttributeType(groups,Map)"))
//        },
//        test("isNumberSet") {
//          val ex = Student.groups.isNumberSet
//          assert(ex.toString)(equalTo("AttributeType(groups,NumberSet)"))
//        },
//        test("isNull") {
//          val ex = Student.groups.isNull
//          assert(ex.toString)(equalTo("AttributeType(groups,Null)"))
//        },
//        test("isStringSet") {
//          val ex = Student.groups.isStringSet
//          assert(ex.toString)(equalTo("AttributeType(groups,StringSet)"))
//        },
//        test("beginsWith") {
//          val ex = Student.email.beginsWith("avi")
//          assert(ex.toString)(equalTo("BeginsWith(email,String(avi))"))
//        },
//        test("between using number range") {
//          val ex = Student.studentNumber.between(1, 3)
//          assert(ex.toString)(equalTo("Between(ProjectionExpressionOperand(studentNumber),Number(1),Number(3))"))
//        },
//        test("inSet for a collection attribute") {
//          val ex = Student.addresses.inSet(Set(List("Addr1")))
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(addresses),Set(List(Chunk(String(Addr1)))))"))
//        },
//        test("inSet for a scalar") {
//          val ex = Student.studentNumber.inSet(Set(1))
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(studentNumber),Set(Number(1)))"))
//        },
//        test("'in' for a collection attribute") {
//          val ex = Student.groups.in(Set("group1"), Set("group2"))
//          assert(ex.toString)(
//            equalTo("In(ProjectionExpressionOperand(groups),Set(StringSet(Set(group2)), StringSet(Set(group1))))")
//          )
//        },
//        test("'in' for a scalar") {
//          val ex = Student.studentNumber.in(1, 2)
//          assert(ex.toString)(equalTo("In(ProjectionExpressionOperand(studentNumber),Set(Number(2), Number(1)))"))
//        },
//        test("'in' for a sum type") {
//          val ex = Student.payment.in(Payment.CreditCard, Payment.PayPal)
//          assert(ex.toString)(
//            equalTo("In(ProjectionExpressionOperand(payment),Set(String(PayPal), String(CreditCard)))")
//          )
//        },
//        test("string contains") {
//          val ex = Student.collegeName.contains("foo")
//          assert(ex.toString)(equalTo("Contains(collegeName,String(foo))"))
//        },
//        test("set contains") {
//          val ex = Student.groups.contains("group1")
//          assert(ex.toString)(equalTo("Contains(groups,String(group1))"))
//        }
//      )
//
//    private val typeSafeComparisonSuite =
//      suite("type safe comparison suite")(
//        test("PE === X") {
//          val ex = Student.studentNumber === 1
//          assert(ex.toString)(equalTo("Equals(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE === PE") {
//          val ex = Student.studentNumber === Student.studentNumber
//          assert(ex.toString)(
//            equalTo("Equals(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE <> X") {
//          val ex = Student.studentNumber <> 1
//          assert(ex.toString)(equalTo("NotEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE <> PE") {
//          val ex = Student.studentNumber <> Student.studentNumber
//          assert(ex.toString)(
//            equalTo("NotEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE < X") {
//          val ex = Student.studentNumber < 1
//          assert(ex.toString)(equalTo("LessThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))"))
//        },
//        test("PE < PE") {
//          val ex = Student.studentNumber < Student.studentNumber
//          assert(ex.toString)(
//            equalTo("LessThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))")
//          )
//        },
//        test("PE <= X") {
//          val ex = Student.studentNumber <= 1
//          assert(ex.toString)(
//            equalTo("LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE <= PE") {
//          val ex = Student.studentNumber <= Student.studentNumber
//          assert(ex.toString)(
//            equalTo(
//              "LessThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        },
//        test("PE > X") {
//          val ex = Student.studentNumber > 1
//          assert(ex.toString)(
//            equalTo("GreaterThan(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE > PE") {
//          val ex = Student.studentNumber > Student.studentNumber
//          assert(ex.toString)(
//            equalTo(
//              "GreaterThan(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        },
//        test("PE >= X") {
//          val ex = Student.studentNumber >= 1
//          assert(ex.toString)(
//            equalTo("GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ValueOperand(Number(1)))")
//          )
//        },
//        test("PE >= PE") {
//          val ex = Student.studentNumber >= Student.studentNumber
//          assert(ex.toString)(
//            equalTo(
//              "GreaterThanOrEqual(ProjectionExpressionOperand(studentNumber),ProjectionExpressionOperand(studentNumber))"
//            )
//          )
//        }
//      )
//
//    val typeSafeSuite =
//      suite("type safe suite")(
//        typeSafeOpSuite,
//        typeSafeFunctionsSuite,
//        typeSafeComparisonSuite
//      )
//  }
//
//}
