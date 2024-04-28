package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.assertTrue
import zio.test.Assertion._
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBQuery.{ deleteFrom, forEach, get, put, putItem, scanAll, update }
import zio.dynamodb.syntax._
import zio.Chunk
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import zio.stream.ZStream
import zio.ZIO
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

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

  override def spec =
    suite("TypeSafeApiCrudSpec")(
      putSuite,
      updateSuite,
      deleteSuite,
      forEachSuite,
      transactionSuite
    ) @@ TestAspect.nondeterministic

  private val putSuite =
    suite("put")(
      test("with ALL_OLD return values should return the old item") {
        withSingleIdKeyTable { tableName =>
          val originalPerson = Person("1", "Smith", Some("John"), 21)
          val updatedPerson  = Person("1", "Smith", Some("Smith"), 42)
          for {
            _       <- put(tableName, originalPerson).execute
            rtrn    <- put(tableName, updatedPerson).returns(ReturnValues.AllOld).execute
            updated <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(rtrn == Some(originalPerson) && updated == updatedPerson)
        }
      },
      test("and get simple round trip") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          for {
            _ <- put(tableName, person).execute
            p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(p == person)
        }
      },
      test("and get using maybeFound extension method") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          for {
            _               <- put(tableName, person).execute
            personFound     <- get(tableName)(Person.id.partitionKey === "1").execute.maybeFound
            personNotFound  <- get(tableName)(Person.id.partitionKey === "DOES_NOT_EXIST").execute.maybeFound
            _               <- putItem(tableName, Item("id" -> "WILL_GIVE_DECODE_ERROR")).execute
            decodeErrorExit <-
              get(tableName)(Person.id.partitionKey === "WILL_GIVE_DECODE_ERROR").execute.maybeFound.exit
          } yield assertTrue(personFound == Some(person) && personNotFound == None) && assert(decodeErrorExit)(
            fails(
              equalTo(
                DynamoDBError.ItemError.DecodingError(
                  "field 'surname' not found in Map(Map(String(id) -> String(WILL_GIVE_DECODE_ERROR)))"
                )
              )
            )
          )
        }
      },
      test("with condition expression that id not exists fails when item exists") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          val exit   = for {
            _    <- put(tableName, person).execute
            exit <- put(tableName, person).where(Person.id.notExists).execute.exit
          } yield exit
          assertZIO(exit)(fails(isConditionalCheckFailedException))
        }
      },
      test("map error from condition expression that id not exists fails when item exists") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          val exit   = for {
            _    <- put(tableName, person).execute
            exit <- put(tableName, person)
                      .where(Person.id.notExists)
                      .execute
                      .mapError {
                        case DynamoDBError.AWSError(_: ConditionalCheckFailedException) => 42
                        case _                                                          => 0
                      }
                      .exit
          } yield (exit)
          assertZIO(exit)(fails(equalTo(42)))
        }
      },
      test("with condition expression that id exists when there is a item succeeds") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          val exit   = for {
            _    <- put(tableName, person).execute
            exit <- put(tableName, person).where(Person.id.exists).execute.exit
          } yield exit
          assertZIO(exit)(succeeds(anything))
        }
      },
      test("with compound condition expression succeeds") {
        withSingleIdKeyTable { tableName =>
          val person        = Person("1", "Smith", None, 21)
          val personUpdated = person.copy(forename = Some("John"))
          for {
            _ <- put(tableName, person).execute
            _ <-
              put(tableName, personUpdated)
                .where(Person.id.exists && Person.surname === "Smith" && Person.forename.notExists && Person.age > 20)
                .execute
            p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(p == personUpdated)
        }
      },
      test("with forEach, catching a BatchError and resuming processing") {
        withSingleIdKeyTable { tableName =>
          type FailureWrapper = Either[String, Option[Person]]
          val person1                                                                = Person("1", "Smith", Some("John"), 21)
          val person2                                                                = Person("2", "Brown", None, 42)
          val inputStream                                                            = ZStream(person1, person2)
          val outputStream: ZStream[DynamoDBExecutor, DynamoDBError, FailureWrapper] = inputStream
            .grouped(2)
            .mapZIO { chunk =>
              val batchWriteItem = DynamoDBQuery
                .forEach(chunk)(a => put(tableName, a))
                .map(Chunk.fromIterable)
              for {
                r <- ZIO.environment[DynamoDBExecutor]
                b <- batchWriteItem.execute.provideEnvironment(r).map(_.map(Right(_))).catchSome {
                       // example of catching a BatchError and resuming processing
                       case DynamoDBError.BatchError.WriteError(map) => ZIO.succeed(Chunk(Left(map.toString)))
                     }
              } yield b
            }
            .flattenChunks

          for {
            xs <- outputStream.runCollect
          } yield assertTrue(xs == Chunk(Right(None), Right(None)))
        }
      }
    )

  def isConditionalCheckFailedException: Assertion[Any] =
    isSubtype[DynamoDBError.AWSError](
      hasField(
        "cause",
        _.cause,
        isSubtype[ConditionalCheckFailedException](anything)
      )
    )

  private val updateSuite = suite("update")(
    // Note ATM both ReturnValues.UpdatedNew and ReturnValues.UpdatedOld will potentially cause a decode error for the high level API
    // if all the attributes are not updated as this will result in partial data being returned and hence a decode error -
    // so should not be use. If these are required then use the low level API for now
    test("with ALL_OLD return values should return the old item") {
      withSingleIdKeyTable { tableName =>
        val originalPerson = Person("1", "Smith", None, 21)
        val updatedPerson  = originalPerson.copy(forename = Some("John"))
        for {
          _    <- put(tableName, originalPerson).execute
          rtrn <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
                    .returns(ReturnValues.AllOld)
                    .execute
          p    <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(rtrn == Some(originalPerson) && p == updatedPerson)
      }
    },
    test("with ALL_NEW return values should return the new item") {
      withSingleIdKeyTable { tableName =>
        val originalPerson = Person("1", "Smith", None, 21)
        val updatedPerson  = originalPerson.copy(forename = Some("John"))
        for {
          _    <- put(tableName, originalPerson).execute
          rtrn <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
                    .returns(ReturnValues.AllNew)
                    .execute
          p    <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(rtrn == Some(updatedPerson) && p == updatedPerson)
      }
    },
    test("sets a single field with an update expression when item exists") {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John"))).execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test("sets a single field with an update expression with a condition expression") {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
                 .where(Person.id === "1")
                 .execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test("set of a single field update with a condition expression that item exists fails for empty database") {
      withSingleIdKeyTable { tableName =>
        val exit =
          update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
            .where(Person.id.exists)
            .execute
            .exit
        assertZIO(exit)(fails(isConditionalCheckFailedException))
      }
    },
    test("set's a single field with an update plus a condition expression that addressSet contains an element") {
      withSingleIdKeyTable { tableName =>
        val person   = PersonWithCollections("1", "Smith", addressSet = Set("address1"))
        val expected = PersonWithCollections("1", "Brown", addressSet = Set("address1"))
        for {
          _ <- put(tableName, person).execute
          _ <-
            update(tableName)(PersonWithCollections.id.partitionKey === "1")(PersonWithCollections.surname.set("Brown"))
              .where(PersonWithCollections.addressSet.contains("address1"))
              .execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test("set's a single field with an update plus a condition expression that addressSet has size 1") {
      withSingleIdKeyTable { tableName =>
        val person   = PersonWithCollections("1", "Smith", addressSet = Set("address1"))
        val expected = PersonWithCollections("1", "Brown", addressSet = Set("address1"))
        for {
          _ <- put(tableName, person).execute
          _ <-
            update(tableName)(PersonWithCollections.id.partitionKey === "1")(PersonWithCollections.surname.set("Brown"))
              .where(PersonWithCollections.id.size === 1)
              .execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test("set's a single field with an update plus a condition expression that surname has size 5") {
      withSingleIdKeyTable { tableName =>
        val person   = PersonWithCollections("1", "Smith", addressSet = Set("address1"))
        val expected = PersonWithCollections("1", "Brown", addressSet = Set("address1"))
        for {
          _ <- put(tableName, person).execute
          _ <-
            update(tableName)(PersonWithCollections.id.partitionKey === "1")(PersonWithCollections.surname.set("Brown"))
              .where(PersonWithCollections.surname.size === 5)
              .execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "set's a single field with an update plus a condition expression that optional forename contains a substring"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", Some("John"), 21)
        val expected = person.copy(age = 22)
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.age.set(22))
                 .where(Person.forename.contains("oh"))
                 .execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "set's a single field with an update expression restricted by a compound condition expression when item exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
                 .where(Person.surname === "Smith" && Person.forename.notExists)
                 .execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "setIfNotExists updates single field when attribute does not exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("Tarlochan"))
        for {
          _ <- put(tableName, person).execute
          _ <-
            update(tableName)(Person.id.partitionKey === "1")(Person.forename.setIfNotExists(Some("Tarlochan"))).execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "setIfNotExists fails silently when the attribute already exists"                    // this is AWS API behaviour
    ) {
      withSingleIdKeyTable { tableName =>
        val person = Person("1", "Smith", None, 21)
        for {
          _    <- put(tableName, person).execute
          exit <- update(tableName)(Person.id.partitionKey === "1")(Person.surname.setIfNotExists("XXXX")).execute.exit
          p    <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(exit.isSuccess == true && p == person)
      }
    },
    test(
      "set's multiple fields with a compound update expression restricted by a compound condition expression where item exists"
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
          p <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test("fails when a single condition expression on primary key equality fails") {
      withSingleIdKeyTable { tableName =>
        val exit = update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
          .where(Person.id === "1")
          .execute
          .exit
        assertZIO(exit)(fails(isConditionalCheckFailedException))
      }
    },
    test(
      "set a map element with a condition expression that the map entry exists and an optics expression on postcode"
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
               ).where(
                 PersonWithCollections.addressMap.valueAt(address1.number).exists &&
                   PersonWithCollections.addressMap.valueAt(address1.number) >>> Address.postcode === "AAAA"
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "set a map element with a condition expression that the map entry does not exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val address1 = Address("1", "AAAA")
        val person   = PersonWithCollections("1", "Smith")
        val expected = person.copy(addressMap = Map(address1.number -> address1))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(PersonWithCollections.id.partitionKey === "1")(
                 PersonWithCollections.addressMap.valueAt(address1.number).set(address1)
               ).where(PersonWithCollections.addressMap.valueAt(address1.number).notExists).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "set a map element"
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
      "remove a map element"
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
      "remove'ing a map element when it does not exists fails silently"                    // this is AWS API behaviour
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
      "set an existing map element"
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
      "append adds an Address element to addressList field"
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
      "appendList adds an Address list to addressList field"
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
    test(
      "remove(1) removes 2nd Address element with condition expression that uses optics"
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
               ).where(
                 PersonWithCollections.addressList.elementAt(1).exists &&
                   PersonWithCollections.addressList.elementAt(1) >>> Address.postcode === "BBBB"
               ).execute
          p <- get(tableName)(PersonWithCollections.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "remove(1) removes 2nd Address element"
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
      "remove(100) on a list of 2 elements fails silently"                                 // this is AWS API behaviour
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
      "elementAt(1).remove removes 2nd Address element"
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
      "prepend adds an Address element to addressList field"
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
      "prependList adds an Address list element to addressList field"
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
      "addSet adds a set of strings to addressSet field"
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
      "deleteFromSet removes a set of strings from addressSet field"
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
      "deleteFromSet fails silently when trying to remove an element that does not exists" // this is AWS API behaviour
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
      "add adds a number to a numeric field if it exists"
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

  private val deleteSuite = suite("delete")(
    test("with ALL_OLD return values should return the old item") {
      withSingleIdKeyTable { tableName =>
        val originalPerson = Person("1", "Smith", Some("John"), 21)
        for {
          _    <- put(tableName, originalPerson).returns(ReturnValues.AllOld).execute
          rtrn <- deleteFrom(tableName)(Person.id.partitionKey === "1").returns(ReturnValues.AllOld).execute
        } yield assertTrue(rtrn == Some(originalPerson))
      }
    },
    test(
      "with an id exists condition expression, followed by a get, confirms item has been deleted"
    ) {
      withSingleIdKeyTable { tableName =>
        val person = Person("1", "Smith", Some("John"), 21)
        for {
          _ <- put(tableName, person).execute
          _ <- deleteFrom(tableName)(Person.id.partitionKey === "1").where(Person.id.exists).execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute
        } yield assertTrue(
          p == Left(ItemError.ValueNotFound("value with key AttrMap(Map(id -> String(1))) not found"))
        )
      }
    },
    test(
      "with id exists condition expression, fails when item does not exist"
    ) {
      withSingleIdKeyTable { tableName =>
        assertZIO(deleteFrom(tableName)(Person.id.partitionKey === "1").where(Person.id.exists).execute.exit)(
          fails(isConditionalCheckFailedException)
        )
      }
    },
    test(
      "with forename, surname and age condition expression, succeeds"
    ) {
      withSingleIdKeyTable { tableName =>
        val person = Person("1", "Smith", Some("John"), 21)
        for {
          _ <- put(tableName, person).execute
          _ <- deleteFrom(tableName)(Person.id.partitionKey === "1")
                 .where(Person.surname === "Smith" && Person.forename === Some("John") && Person.age >= 21)
                 .execute
          p <- get(tableName)(Person.id.partitionKey === "1").execute
        } yield assertTrue(
          p == Left(ItemError.ValueNotFound("value with key AttrMap(Map(id -> String(1))) not found"))
        )
      }
    }
  )

  // note `forEach` will result in auto batching of the query if it is a get, put or a delete
  private val forEachSuite = suite("forEach")(
    test("with a get query returns Right of Person when item exists") {
      withSingleIdKeyTable { tableName =>
        val person1 = Person("1", "Smith", Some("John"), 21)
        val person2 = Person("2", "Brown", Some("Peter"), 42)
        for {
          _      <- put(tableName, person1).execute
          _      <- put(tableName, person2).execute
          people <- forEach(Chunk(person1, person2))(p => get(tableName)(Person.id.partitionKey === p.id)).execute
        } yield assertTrue(people == List(Right(person1), Right(person2)))
      }
    },
    test("with a get query returns Left of ValueNotFound when an item does not exist") {
      withSingleIdKeyTable { tableName =>
        val person1 = Person("1", "Smith", Some("John"), 21)
        val person2 = Person("2", "Brown", Some("Peter"), 42)
        for {
          people <- forEach(Chunk(person1, person2))(p => get(tableName)(Person.id.partitionKey === p.id)).execute
        } yield assertTrue(
          people == List(
            Left(ItemError.ValueNotFound("value with key AttrMap(Map(id -> String(1))) not found")),
            Left(ItemError.ValueNotFound("value with key AttrMap(Map(id -> String(2))) not found"))
          )
        )
      }
    },
    test("with a put query") {
      withSingleIdKeyTable { tableName =>
        val person1 = Person("1", "Smith", Some("John"), 21)
        val person2 = Person("2", "Jones", Some("Tarlochan"), 42)
        for {
          _      <- forEach(Chunk(person1, person2))(person => put(tableName, person)).execute
          stream <- scanAll[Person](tableName).execute
          people <- stream.runCollect
        } yield assertTrue(people.sortBy(_.id) == Chunk(person1, person2))
      }
    },
    test("with a delete query") {
      withSingleIdKeyTable { tableName =>
        val person1 = Person("1", "Smith", Some("John"), 21)
        val person2 = Person("2", "Brown", Some("Peter"), 42)
        for {
          _      <- put(tableName, person1).execute
          _      <- put(tableName, person2).execute
          _      <- forEach(Chunk(person1, person2))(person =>
                      deleteFrom(tableName)(Person.id.partitionKey === person.id)
                    ).execute
          stream <- scanAll[Person](tableName).execute
          people <- stream.runCollect
        } yield assertTrue(people == Chunk.empty)
      }
    },
    test("with an update query") { // not there is no AWS API for batch update so these queries are run in parallel
      withSingleIdKeyTable { tableName =>
        val person1 = Person("1", "Smith", Some("John"), 21)
        val person2 = Person("2", "Brown", Some("Peter"), 42)
        for {
          _      <- put(tableName, person1).execute
          _      <- put(tableName, person2).execute
          _      <- forEach(Chunk(person1, person2))(person =>
                      update(tableName)(Person.id.partitionKey === person.id)(Person.age.add(1))
                    ).execute
          stream <- scanAll[Person](tableName).execute
          people <- stream.runCollect
        } yield assertTrue(people.sortBy(_.id) == Chunk(person1.copy(age = 22), person2.copy(age = 43)))
      }
    }
  )

  val transactionSuite = suite("transactions")(
    test("multiple get queries succeed (ie no AWS failures) within a transaction") {
      withSingleIdKeyTable { tableName =>
        val person1      = Person("1", "Smith", Some("John"), 21)
        val person2      = Person("2", "Jones", Some("Tarlochan"), 42)
        val getJohn      = get(tableName)(Person.id.partitionKey === "1")
        val getTarlochan = get(tableName)(Person.id.partitionKey === "2")
        for {
          _      <- forEach(Chunk(person1, person2))(person => put(tableName, person)).execute
          result <- (getJohn zip getTarlochan).transaction.execute.either
        } yield assert(result)(isRight(equalTo((Right(person1), Right(person2)))))
      }
    },
    test("multiple puts are atomic within a transaction") {
      withSingleIdKeyTable { tableName =>
        val person1    = Person("1", "Smith", Some("John"), 21)
        val person2    = Person("2", "Jones", Some("Peter"), 42)
        val putPerson1 = put(tableName, person1.copy(forename = Some("Updated"))).where(Person.id <> "2")
        val putPerson2 = put(tableName, person2.copy(forename = Some("Updated"))).where(Person.id <> "2")
        for {
          _         <- forEach(Chunk(person1, person2))(person => put(tableName, person)).execute
          result    <- (putPerson1 zip putPerson2).transaction.execute.either
          hasTXError = result match {
                         case Left(DynamoDBError.AWSError(_: TransactionCanceledException)) => true
                         case _                                                             => false
                       }
          stream    <- scanAll[Person](tableName).execute
          people    <- stream.runCollect
        } yield assertTrue(hasTXError) && assertTrue(!people.exists(_.forename == Some("Updated")))
      // without the transaction the 1st put would have succeeded
      }
    }
  )

}
