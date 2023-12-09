package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.Assertion._
import zio.schema.annotation.noDiscriminator
import zio.dynamodb.DynamoDBQuery.{ get, put, update }
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException

// This is a place holder suite for the Type Safe API for now, to be expanded upon in the future
object TypeSafeApiCrudSpec extends DynamoDBLocalSpec {

  final case class Person(id: String, surname: String, forename: Option[String], age: Int)
  object Person {
    implicit val schema: Schema.CaseClass4[String, String, Option[String], Int, Person] = DeriveSchema.gen[Person]
    val (id, surname, forename, age)                                                    = ProjectionExpression.accessors[Person]
  }

  final case class Address(number: String, Postcode: String)
  object Address               {
    implicit val schema: Schema.CaseClass2[String, String, Address] = DeriveSchema.gen[Address]
    val (number, postcode)                                          = ProjectionExpression.accessors[Address]
  }
  final case class PersonWithCollections(
    id: String,
    surname: String,
    addressList: List[Address] = List.empty,
    addressMap: Map[String, Address] = Map.empty,
    addressSet: Set[String] = Set.empty
  )
  object PersonWithCollections {
    implicit val schema
      : Schema.CaseClass5[String, String, List[Address], Map[String, Address], Set[String], PersonWithCollections] =
      DeriveSchema.gen[PersonWithCollections]
    val (id, surname, addressList, addressMap, addressSet)                                                         = ProjectionExpression.accessors[PersonWithCollections]
  }

  @noDiscriminator
  sealed trait NoDiscrinatorSumType {
    def id: String
  }
  object NoDiscrinatorSumType       {
    final case class One(id: String, i: Int) extends NoDiscrinatorSumType
    object One {
      implicit val schema: Schema.CaseClass2[String, Int, One] = DeriveSchema.gen[One]
      val (id, i)                                              = ProjectionExpression.accessors[One]
    }
    final case class Two(id: String, j: Int) extends NoDiscrinatorSumType
    object Two {
      implicit val schema: Schema.CaseClass2[String, Int, Two] = DeriveSchema.gen[Two]
      val (id, j)                                              = ProjectionExpression.accessors[Two]
    }
  }

  override def spec = suite("all")(putSuite, updateSuite) @@ TestAspect.nondeterministic

  private val putSuite =
    suite("TypeSafeApiSpec")(
      test("'put' and get simple round trip") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          for {
            _ <- put(tableName, person).execute
            p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(p == person)
        }
      },
      test("'put' with condition expression that id exists fails for empty database") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          val exit   = for {
            _    <- put(tableName, person).execute
            exit <- put(tableName, person).where(Person.id.notExists).execute.exit
          } yield exit
          assertZIO(exit)(fails(isSubtype[ConditionalCheckFailedException](anything)))
        }
      },
      test("'put' with condition expression that id exists when there is a record succeeds") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          val exit   = for {
            _    <- put(tableName, person).execute
            exit <- put(tableName, person).where(Person.id.exists).execute.exit
          } yield exit
          assertZIO(exit)(succeeds(anything))
        }
      },
      test("'put' with compound condition expression succeeds") {
        withSingleIdKeyTable { tableName =>
          val person        = Person("1", "Smith", None, 21)
          val personUpdated = person.copy(forename = Some("John"))
          for {
            _ <- put(tableName, person).execute
            _ <-
              put(tableName, personUpdated)
                .where(Person.id.exists && Person.surname === "Smith" && Person.forename.notExists && Person.age > 20)
                .execute
            p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(p == personUpdated)
        }
      }
    )

  private val updateSuite = suite("update's")(
    test("'sets a single field with an update expression when record exists") {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John"))).execute
          p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    /*
AliasMap(
  Map(
    AttributeValueKey(String(John)) -> :v0,
    PathSegment(,forename) -> #n1,
    PathSegment(,id) -> #n2,
    AttributeValueKey(String(1)) -> :v3
  ),
  4
)
updateExpr: set #n1 = :v0
conditionExpr: Some((#n2) = (:v3))
     */
    test("'sets a single field with an update expression with a condition expression") {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
                 .where(Person.id === "1")
                 .execute
          p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    // TODO: Avi - see if we can fix underlying updateItem to return an error in this case
    test("fails when a record when it does not exists") {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        val exit     = for {
          _    <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John"))).execute
          exit <- get[Person](tableName)(Person.id.partitionKey === "1").execute.exit
        } yield exit
        assertZIO(exit)(fails(isSubtype[ConditionalCheckFailedException](anything)))
      }
    } @@ TestAspect.ignore,
    test(
      "'set's a single field with an update expression restricted by a compound condition expression when record exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
                 .where(Person.surname === "Smith" && Person.forename.notExists)
                 .execute
          p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'setIfNotExists' updates single field when attribure does not exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("Tarlochan"))
        for {
          _ <- put(tableName, person).execute
          _ <-
            update(tableName)(Person.id.partitionKey === "1")(Person.forename.setIfNotExists(Some("Tarlochan"))).execute
          p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'setIfNotExists' fails silently when the attribute already exists"                     // this is AWS API behaviour
    ) {
      withSingleIdKeyTable { tableName =>
        val person = Person("1", "Smith", None, 21)
        for {
          _    <- put(tableName, person).execute
          exit <- update(tableName)(Person.id.partitionKey === "1")(Person.surname.setIfNotExists("XXXX")).execute.exit
          p    <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(exit.isSuccess == true && p == person)
      }
    },
    test(
      "'set's multiple fields with a compound update expression restricted by a compound condition expression where record exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"), surname = "John")
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(
                 Person.forename.set(Some("John")) + Person.surname.set("John")
               )
                 .where(Person.surname === "Smith" && Person.forename.notExists)
                 .execute
          p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test("fails when a single condition expression on primary key equality fails") {
      withSingleIdKeyTable { tableName =>
        val exit = update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
          .where(Person.id === "1")
          .execute
          .exit
        assertZIO(exit)(fails(isSubtype[ConditionalCheckFailedException](anything)))
      }
    },
    /*
XXXXXX final aliasMap = AliasMap(Map(AttributeValueKey(Map(Map(String(Postcode) -> String(BBBB), String(number) -> String(1)))) -> :v0, PathSegment(addressMap,1) -> #n1, PathSegment(,addressMap) -> #n2),3), xs = List(.#n1, #n2)
XXXXXX final aliasMap = AliasMap(Map(AttributeValueKey(Map(Map(String(Postcode) -> String(BBBB), String(number) -> String(1)))) -> :v0, PathSegment(addressMap,1) -> #n3, PathSegment(,addressMap) -> #n2),4), xs = List(.#n3)

AliasMap(
  Map(
    AttributeValueKey(Map(Map(String(Postcode) -> String(BBBB), String(number) -> String(1)))) -> :v0,
    PathSegment(addressMap,1) -> #n3,
    PathSegment(,addressMap) -> #n2),
  4
)
updateExpr: set #n2.#n1 = :v0
conditionExpr: Some(attribute_exists(.#n3))

    Exception in thread "zio-fiber-71" software.amazon.awssdk.services.dynamodb.model.DynamoDbException:
    Invalid UpdateExpression: An expression attribute name used in the document path is not defined; attribute name: #n1
    (Service: DynamoDb, Status Code: 400, Request ID: cf71da83-41cd-4c26-89e7-999ffdb85ef6)
     */
    test(
      "'set' a map element with a condition expression 1"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("1", "BBBB")
        val person   = PersonWithCollections("1", "Smith", addressMap = Map(address1.number -> address1))
        val expected = person.copy(addressMap = Map(address1.number -> address2))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressMap.valueAt(address1.number).set(address2)
               ).where(PersonWithCollections.addressMap.valueAt(address1.number).exists).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'set' a map element with a condition expression 2"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("1", "BBBB")
        val person   = PersonWithCollections("1", "Smith", addressMap = Map(address1.number -> address1))
        val expected = person.copy(surname = "XXXX")
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.surname.set("XXXX")
               ).where(PersonWithCollections.addressMap.valueAt(address1.number).exists).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'set' a map element"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val person   = PersonWithCollections("1", "Smith")
        val expected = person.copy(addressMap = Map(address1.number -> address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressMap.valueAt(address1.number).set(address1)
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'remove' a map element"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val person   = PersonWithCollections(
          "1",
          "Smith",
          addressMap = Map(address1.number -> address1, address2.number -> address2)
        )
        val expected = person.copy(addressMap = Map(address1.number -> address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressMap.valueAt(address2.number).remove
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'remove'ing a map element when it does not exists fails silently"                      // this is AWS API behaviour
    ) {
      withSingleIdKeyTable { tableName =>
        val person = PersonWithCollections(
          "1",
          "Smith"
        )
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressMap.valueAt("DOES_NOT_EXIST").remove
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == person)
      }
    },
    test(
      "'set' an existing map element"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1        = Address("1", "AAAA")
        val address1Updated = Address("1", "BBBB")
        val person          = PersonWithCollections(
          "1",
          "Smith",
          addressMap = Map(address1.number -> address1)
        )
        val expected        = person.copy(addressMap = Map(address1.number -> address1Updated))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressMap.valueAt(address1.number).set(address1Updated)
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'append' adds an Address element to addressList field"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1))
        val expected = person.copy(addressList = List(address1, address2))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.append(address2)
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'appendList' adds an Address list to addressList field"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val address3 = Address("3", "CCCC")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1))
        val expected = person.copy(addressList = List(address1, address2, address3))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.appendList(List(address2, address3))
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    /*
      Exception in thread "zio-fiber-71" software.amazon.awssdk.services.dynamodb.model.DynamoDbException:
      Invalid ConditionExpression: Syntax error; token: "[", near: "([1" (Service: DynamoDb, Status Code: 400, Request ID: b73fb977-5348-4540-bf7b-044c183bc0f6)
     */
    test(
      "'remove(1)' removes 2nd Address element with condition expression"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1, address2))
        val expected = person.copy(addressList = List(address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.remove(1)
               ).where(PersonWithCollections.addressList.elementAt(1).exists).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'remove(1)' removes 2nd Address element"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1, address2))
        val expected = person.copy(addressList = List(address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.remove(1)
               ).where(PersonWithCollections.addressList.elementAt(1).exists).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'remove(100)' on a list of 2 elements fails silently"                                  // this is AWS API behaviour
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.remove(100)
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == person)
      }
    },
    test(
      "'elementAt(1).remove' removes 2nd Address element"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1, address2))
        val expected = person.copy(addressList = List(address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.elementAt(1).remove
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'prepend' adds an Address element to addressList field"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1))
        val expected = person.copy(addressList = List(address2, address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.prepend(address2)
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'prependList' adds an Address list element to addressList field"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val address2 = Address("2", "BBBB")
        val address3 = Address("3", "CCCC")
        val person   = PersonWithCollections("1", "Smith", addressList = List(address1))
        val expected = person.copy(addressList = List(address2, address3, address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressList.prependList(List(address2, address3))
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'addSet' adds a set of strings to addressSet field"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = PersonWithCollections("1", "Smith", addressSet = Set("address1"))
        val expected = person.copy(addressSet = Set("address2", "address3", "address1"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressSet.addSet(Set("address2", "address3"))
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'deleteFromSet' removes a set of strings from addressSet field"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = PersonWithCollections("1", "Smith", addressSet = Set("address2", "address3", "address1"))
        val expected = person.copy(addressSet = Set("address1"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressSet.deleteFromSet(Set("address2", "address3"))
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "'deleteFromSet' fails silently when trying to remove an elements that does not exists" // this is AWS API behaviour
    ) {
      withSingleIdKeyTable { tableName =>
        val person = PersonWithCollections("1", "Smith", addressSet = Set("address1"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressSet.deleteFromSet(Set("address2"))
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == person)
      }
    },
    test(
      "'add' adds a number to a numeric field if it exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", Some("John"), 21)
        val expected = person.copy(age = 22)
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(
                 Person.age.add(1)
               ).execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    }
  )

}
