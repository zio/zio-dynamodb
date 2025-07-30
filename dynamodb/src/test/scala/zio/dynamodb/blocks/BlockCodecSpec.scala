package zio.dynamodb.blocks

import zio.dynamodb.{
  AttrMap,
  AttributeValue,
  ConditionExpression,
  KeyConditionExpr,
  PartitionKey,
  ProjectionExpression,
  UpdateExpression
}
import zio.blocks.schema._
import zio.dynamodb.KeyConditionExpr.PartitionKeyEquals
import zio.test._
import zio.dynamodb.blocks.BlockCodecSpec.PersonWithCollections.addressNumberAt

/*
OPTICS
- optional fields and navigation
- Collections - List, Map, Set

 */
object BlockCodecSpec extends ZIOSpecDefault {

  // Example from DerivationBuilder ScalaDoc
  object DeriveExperiments {
    trait Eq[A] extends Serializable { self =>
      def eqv(x: A, y: A): Boolean
    }
    object Eq {
      def apply[A](implicit ev: Eq[A]): Eq[A] = ev
      implicit val intEq: Eq[Int]             = new Eq[Int] {
        def eqv(x: Int, y: Int): Boolean = x == y
      }
    }
    val personSchema: Schema[PersonWithName] = PersonWithName.schema
    // derivation is tied to Format ATM??? TODO: get this working
    //val personEq = personSchema.deriving[Eq](null).instance(PersonWithName.age, Eq[Int]).derive
  }

  sealed trait Payment // mixture of case objects and case classes
  case object Cash                   extends Payment                    {
    implicit val schema: Schema[Cash.type] = Schema.derived
  }
  case object CreditCard             extends Payment                    {
    implicit val schema: Schema[CreditCard.type] = Schema.derived
  }
  final case class DebitCard(i: Int) extends Payment
  object DebitCard                   extends CompanionOptics[DebitCard] {
    implicit val schema: Schema[DebitCard] = Schema.derived
    val i: Lens[DebitCard, Int]            = optic(_.i)
  }
  object Payment                     extends CompanionOptics[Payment]   {
    implicit val schema: Schema[Payment]            = Schema.derived
    val cash: Prism[Payment, Cash.type]             = optic(_.when[Cash.type])
    val creditCard: Prism[Payment, CreditCard.type] = optic(_.when[CreditCard.type])
    val debitCard: Prism[Payment, DebitCard]        = optic(_.when[DebitCard])
  }
  final case class Person(id: String, payment: Payment)
  object Person                      extends CompanionOptics[Person]    {
    implicit val schema: Schema[Person]        = Schema.derived
    val id: Lens[Person, String]               = optic(_.id)
    val payment: Lens[Person, Payment]         = optic(_.payment)
    val debitCardFieldI: Optional[Person, Int] =
      optic(_.payment.when[DebitCard].i) // example of digging into concrete sum type - NICE!!!!!
  }

  final case class PersonWithName(id: String, payment: Payment, name: String = "default", age: Int = 21)
  object PersonWithName extends CompanionOptics[PersonWithName] {
    implicit val schema: Schema[PersonWithName] = Schema.derived
    val id: Lens[PersonWithName, String]        = optic(_.id)
    val payment: Lens[PersonWithName, Payment]  = optic(_.payment)
    val name: Lens[PersonWithName, String]      = optic(_.name)
    val age: Lens[PersonWithName, Int]          = optic(_.age)
  }

  final case class Address(number: String)
  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived
    val number: Lens[Address, String]    = optic(_.number)
  }

  final case class PersonWithCollections(
    id: String,
    addresses: List[Address],
    map: Map[String, Int] = Map.empty,
    maybeAddresses: Option[List[Address]] = None,
    maybeMap: Option[Map[String, Int]] = None,
    maybeNestedMap: Option[Map[String, Option[Map[String, Int]]]] = None
  )
  object PersonWithCollections extends CompanionOptics[PersonWithCollections] {
    implicit val schema: Schema[PersonWithCollections] = Schema.derived
    val id: Lens[PersonWithCollections, String]        = optic(_.id)

    val firstAddress: Optional[PersonWithCollections, Address] = optic(_.addresses.at(0))

    def addressAt(index: Int): Optional[PersonWithCollections, Address]      = optic(_.addresses.at(index))
    def addressNumberAt(index: Int): Optional[PersonWithCollections, String] = optic(_.addresses.at(index).number)
    //def maybeAddressAt(index: Int)      = optic(_.maybeAddresses.each)
    def mapAtKey(key: String): Optional[PersonWithCollections, Int]          = optic(_.map.atKey(key))

    val maybeAddresses: Optional[PersonWithCollections, List[Address]]                        =
      optic(_.maybeAddresses.when[Some[List[Address]]].value)
    def maybeAddressNumberAt(index: Int): Optional[PersonWithCollections, String]             =
      optic(_.maybeAddresses.when[Some[List[Address]]].value.at(index).number)
    def maybeMapAtKey(key: String): Optional[PersonWithCollections, Int]                      =
      optic(_.maybeMap.when[Some[Map[String, Int]]].value.atKey(key))
    def maybeNestedMapAtKey(key1: String, key2: String): Optional[PersonWithCollections, Int] =
      optic(
        _.maybeNestedMap
          .when[Some[Map[String, Option[Map[String, Int]]]]]
          .value
          .atKey(key1)
          .when[Some[Map[String, Int]]]
          .value
          .atKey(key2)
      )
  }

  sealed trait Payment2 // Sum type with case objects only
  object Payment2 extends CompanionOptics[Payment2] {
    final case class Cash2(i: Int)       extends Payment2
    object Cash2                         extends CompanionOptics[Cash2]       {
      implicit val schema: Schema[Cash2] = Schema.derived
      val i: Lens[Cash2, Int]            = optic(_.i)
    }
    final case class CreditCard2(i: Int) extends Payment2
    object CreditCard2                   extends CompanionOptics[CreditCard2] {
      implicit val schema: Schema[CreditCard2] = Schema.derived
      val i: Lens[CreditCard2, Int]            = optic(_.i)
    }

    implicit val schema: Schema[Payment2] = Schema.derived
  }
  final case class Person2(id: String, payment: Payment2)
  object Person2  extends CompanionOptics[Person2]  {
    implicit val schema: Schema[Person2] = Schema.derived
    val id: Lens[Person2, String]        = optic(_.id)
    val payment: Lens[Person2, Payment2] = optic(_.payment)
  }

  final case class PersonOpt(id: String, age: Option[Int] = None)
  object PersonOpt extends CompanionOptics[PersonOpt] {
    implicit val schema: Schema[PersonOpt]     = Schema.derived
    val id: Lens[PersonOpt, String]            = optic(_.id)
    val age: Optional[PersonOpt, Int]          = optic(_.age.when[Some[Int]].value)
    val maybeAge: Lens[PersonOpt, Option[Int]] = optic(_.age)
  }

  final case class PersonWithAddress(id: String, address: Address)
  object PersonWithAddress extends CompanionOptics[PersonWithAddress] {
    implicit val schema: Schema[PersonWithAddress]     = Schema.derived
    val id: Lens[PersonWithAddress, String]            = optic(_.id)
    val address: Lens[PersonWithAddress, Address]      = optic(_.address)
    val addressNumber: Lens[PersonWithAddress, String] = optic(_.address.number)
  }

  @Modifier.config(
    "discriminatorName",
    "paymentType"
  )                     // TODO: see if we can add modifier programmatically to derived schema
  sealed trait Payment3 // Sum type with case classes only
  object Payment3 extends CompanionOptics[Payment3] {
    final case class Cash3(i: Int)       extends Payment3
    object Cash3                         extends CompanionOptics[Cash3]       {
      implicit val schema: Schema[Cash3] = Schema.derived
      val i: Lens[Cash3, Int]            = optic(_.i)
    }
    final case class CreditCard3(i: Int) extends Payment3
    object CreditCard3                   extends CompanionOptics[CreditCard3] {
      implicit val schema: Schema[CreditCard3] = Schema.derived
      val i: Lens[CreditCard3, Int]            = optic(_.i)
    }

    implicit val schema: Schema[Payment3] = Schema.derived
  }
  final case class Person3(id: String, payment: Payment3)
  object Person3  extends CompanionOptics[Person3]  {
    implicit val schema: Schema[Person3] = Schema.derived
    val id: Lens[Person3, String]        = optic(_.id)
    val payment: Lens[Person3, Payment3] = optic(_.payment)
  }

  // see if abstract field anti pattern is possible to implement
  @Modifier.config(
    "discriminatorName",
    "antiPatternType"
  ) // TODO: see if we can add modifier programmatically to derived schema
  sealed trait AbstractFieldAntiPattern {
    def id: String
  }

  val spec = suite("BlockCodecSpec")(
    suite("Covert a sum type Optic to a PE")(
      test("non top level Optic") {
        val debitCardFieldPE: ProjectionExpression[Person, Int] = OpticToPE.pe(Person.debitCardFieldI)
        println(debitCardFieldPE)
        val payment                                             = ProjectionExpression.MapElement[Person, Payment](
          ProjectionExpression.Root,
          "payment"
        )
        val i                                                   = ProjectionExpression.MapElement[Person, Int](
          payment,
          "i"
        )
        assertTrue(debitCardFieldPE == i)
      }
      // TODO - top level Optic
    ),
    suite("Covert an indexed Optic to a PE")(
      test("index a List with 'at'") {
        val addressOptic: Optional[PersonWithCollections, Address]   = PersonWithCollections.addressAt(0)
        val pe: ProjectionExpression[PersonWithCollections, Address] = OpticToPE.pe(addressOptic)
        val pe2: ProjectionExpression[PersonWithCollections, String] = OpticToPE.pe(addressNumberAt(0))

        val map    = ProjectionExpression.MapElement[PersonWithCollections, Address](
          ProjectionExpression.Root,
          "addresses"
        )
        val list   = ProjectionExpression.ListElement[PersonWithCollections, Address](
          map,
          0
        )
        val number = ProjectionExpression.MapElement[PersonWithCollections, String](
          list,
          "number"
        )
        assertTrue(pe == list && pe2 == number)
      },
      test("index a map with 'atKey'") {
        val p: PersonWithCollections                             = PersonWithCollections("1", List(Address("1")), Map("a" -> 1, "b" -> 2))
        val mapValue: Option[Int]                                = PersonWithCollections.mapAtKey("a").getOption(p)
        val pe: ProjectionExpression[PersonWithCollections, Int] = OpticToPE.pe(PersonWithCollections.mapAtKey("a"))
        val expectedPE                                           = ProjectionExpression.MapElement[PersonWithCollections, Int](
          ProjectionExpression.MapElement[PersonWithCollections, Int](
            ProjectionExpression.Root,
            "map"
          ),
          "a"
        )
        assertTrue(mapValue == Some(1) && pe == expectedPE)
      },
      test("index an optional map with 'MapAtKey'") {
        val p: PersonWithCollections =
          PersonWithCollections(id = "1", addresses = List(Address("1")), maybeMap = Some(Map("a" -> 1, "b" -> 2)))
        val mapValue: Option[Int]    = PersonWithCollections.maybeMapAtKey("a").getOption(p)

        val pe: ProjectionExpression[PersonWithCollections, Int] =
          OpticToPE.pe(PersonWithCollections.maybeMapAtKey("a"))
        val expectedPE                                           = ProjectionExpression.MapElement[PersonWithCollections, Int](
          ProjectionExpression.MapElement[PersonWithCollections, Int](
            ProjectionExpression.Root,
            "maybeMap"
          ),
          "a"
        )
        assertTrue(mapValue == Some(1) && pe == expectedPE)
      },
      test("index a NESTED optional map with 'MapAtKey'") {
        val p: PersonWithCollections =
          PersonWithCollections(
            id = "1",
            addresses = List(Address("1")),
            maybeNestedMap = Some(Map("a" -> Some(Map("b" -> 2))))
          )
        val mapValue: Option[Int]    = PersonWithCollections.maybeNestedMapAtKey("a", "b").getOption(p)

        val pe: ProjectionExpression[PersonWithCollections, Int] =
          OpticToPE.pe(PersonWithCollections.maybeNestedMapAtKey("a", "b"))
        val rootPe                                               = ProjectionExpression.MapElement[PersonWithCollections, Int](
          ProjectionExpression.MapElement[PersonWithCollections, Int](
            ProjectionExpression.Root,
            "maybeNestedMap"
          ),
          "a"
        )
        val expectedPE                                           = ProjectionExpression.MapElement[PersonWithCollections, Int](
          rootPe,
          "b"
        )
        assertTrue(mapValue == Some(2) && pe == expectedPE)
      }
    ),
    suite("Optional")(
      test("read and update a simple optional field") {
        val p = PersonOpt("1", Some(21))

        assertTrue(
          PersonOpt.age.replace(p, 22) == PersonOpt("1", Some(22)) &&
            PersonOpt.maybeAge.replace(p, None) == PersonOpt("1", None) &&
            PersonOpt.age.getOption(p) == Some(21) &&
            PersonOpt.age.replaceOrFail(p, 21) == Right(PersonOpt("1", Some(21)))
        )
      },
      test("read Optional Map using mapAtKey") {
        val p =
          PersonWithCollections(id = "1", addresses = List(Address("1")), maybeMap = Some(Map("a" -> 1, "b" -> 2)))

        PersonWithCollections.maybeMapAtKey("a").getOption(p) match {
          case Some(value) => assertTrue(value == 1)
          case None        => assertTrue(false)
        }
      },
      test("read Optional Nested Map using mapAtKey") {
        val p =
          PersonWithCollections(
            id = "1",
            addresses = List(Address("1")),
            maybeNestedMap = Some(Map("a" -> Some(Map("b" -> 2))))
          )

        PersonWithCollections.maybeNestedMapAtKey("a", "b").getOption(p) match {
          case Some(value) => assertTrue(value == 2)
          case None        => assertTrue(false)
        }
      }
    ),
    suite("Encode/Decode")(
      suite("Encode/Decode Case class with a simple enum")(
        test("encodes simple enum") {
          val p        = Person("1", Cash)
          val enc      = BlocksCodec.encoder[Person]
          val expected = AttrMap("id" -> "1", "payment" -> "Cash").toAttributeValue
          assertTrue(enc(p) == expected)
        },
        test("decodes simple enum") {
          val av  = AttrMap("id" -> "1", "payment" -> "Cash").toAttributeValue
          val dec = BlocksCodec.decoder[Person]
          assertTrue(dec(av) == Right(Person("1", Cash)))
        }
      ),
      suite("Encode/Decode Variant that is a product/record without discriminatorName modifier")(
        test("encode variant") {
          val p        = Person2("1", Payment2.Cash2(1))
          val enc      = BlocksCodec.encoder[Person2]
          val expected = AttrMap(
            "id"      -> "1",
            "payment" -> AttrMap(
              "Cash2" -> AttrMap("i" -> 1)
            )
          ).toAttributeValue
          assertTrue(enc(p) == expected)
        },
        test("decodes variant") {
          val expected = Person2("1", Payment2.Cash2(1))
          val av       = AttrMap(
            "id"      -> "1",
            "payment" -> AttrMap(
              "Cash2" -> AttrMap("i" -> 1)
            )
          ).toAttributeValue
          val dec      = BlocksCodec.decoder[Person2]
          assertTrue(dec(av) == Right(expected))
        }
      ),
      suite("Encode/Decode Variant that is a product/record with discriminatorName modifier")(
        test("encode variant") {
          val p        = Person3("1", Payment3.Cash3(1))
          val enc      = BlocksCodec.encoder[Person3]
          val expected = AttrMap(
            "id"      -> "1",
            "payment" -> AttrMap(
              "paymentType" -> "Cash3",
              "i"           -> 1
            )
          ).toAttributeValue
          assertTrue(enc(p) == expected)
        },
        test("decodes variant") {
          val expected = Person3("1", Payment3.Cash3(1))
          val av       = AttrMap(
            "id"      -> "1",
            "payment" -> AttrMap(
              "paymentType" -> "Cash3",
              "i"           -> 1
            )
          ).toAttributeValue
          val dec      = BlocksCodec.decoder[Person3]
          assertTrue(dec(av) == Right(expected))
        }
      )
    ), // end Encode/Decode
    suite("explore Optics")(
      test("use lens") {
        val p  = PersonWithName("1", Cash)
        val id = PersonWithName.id.get(p)
        assertTrue(id == "1")
      },
      test("use prism") {
        val p                = PersonWithName("1", Cash)
        val payment: Payment = PersonWithName.payment.get(p)
        val x: TestResult    = Payment.cash.getOption(payment) match {
          case Some(c) => assertTrue(c == Cash)
          case None    => assertTrue(false)
        }
        x && assertTrue(payment == Cash)
      },
      test("optics to Update expr") {
        import zio.dynamodb.blocks.BlocksApi._

        val updateName: UpdateExpression.Action.SetAction[PersonWithName, String] = PersonWithName.name.set("John")

        // TODO: fix this conversion process
        val pk: KeyConditionExpr.PrimaryKeyExpr[PersonWithName] = PersonWithName.id === "1"
        println(pk)

        assertTrue(
          updateName == UpdateExpression.Action.SetAction[PersonWithName, String](
            ProjectionExpression.MapElement(ProjectionExpression.Root, "name"),
            UpdateExpression.SetOperand.ValueOperand(AttributeValue.String("John"))
          )
        )
      } @@ TestAspect.ignore,
      test("SchemaExpr experiments") {
        import zio.dynamodb.blocks.BlocksApi._

        val ageSchemaExpr: SchemaExpr[PersonWithName, Boolean]  = PersonWithName.age > 21
        val ageSchemaExpr2: SchemaExpr[PersonWithName, Boolean] = PersonWithName.age > 21 && PersonWithName.age < 30
        val nameSchemaExpr: ConditionExpression[PersonWithName] =
          PersonWithName.name beginsWith "John" // via syntax class
        println(s"$ageSchemaExpr2 $ageSchemaExpr $nameSchemaExpr")

        def printConditionExpression[S](c: ConditionExpression[S]): Unit =
          println(s"example condExpr: $c")

        printConditionExpression(PersonWithName.age > 21 || PersonWithName.age < 30)

        // "at" access to collections
        printConditionExpression(PersonWithCollections.addressAt(0) === Address("1"))
        printConditionExpression(PersonWithCollections.addressNumberAt(0) > "1")
        printConditionExpression(
          PersonWithCollections.addressNumberAt(0) beginsWith "1"
        ) // we get "beginsWith" from syntax class for PE
        printConditionExpression(PersonWithCollections.mapAtKey("a") === 1)
        //printConditionExpression(PersonWithCollections.maybeAddresses.listValues === 1)

        // LOW LEVEL API
        // we could keep existing PE -> CE FOR ****LOW LEVEL**** API ONLY
        printConditionExpression(
          (ProjectionExpression.$("X").beginsWith("John") || ProjectionExpression.$("X").beginsWith("John"))
        )
        printConditionExpression(
          ProjectionExpression.$("X").beginsWith("John") && ProjectionExpression.$("X").beginsWith("John")
        )

        assertTrue(true)
      }
    ),
    suite("partitionKey/sortKey")(
      suite("using new API")(
        // TODO: Conjunction/Disjunction of PK with SK
        test("partitionKey equality expression") {
          import zio.dynamodb.blocks.BlocksApi._

          // implicit def in play
          val pkExpr: KeyConditionExpr.PrimaryKeyExpr[PersonWithName] = PersonWithName.id === "1"
          val expectedPkExpr                                          = PartitionKeyEquals(PartitionKey("id"), AttributeValue.String("1"))

          assertTrue(pkExpr == expectedPkExpr)
        },
        test("partitionKey non-equality expression fails") {
          import zio.dynamodb.blocks.BlocksApi._

          try {
            // implicit def in play
            val _ = (PersonWithName.id > "1"): KeyConditionExpr.PrimaryKeyExpr[PersonWithName]
            assertTrue(false) // should not reach here
          } catch {
            case _: Throwable => assertTrue(true) // expected failure
          }
        }
      )
    )
  )

}
