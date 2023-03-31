package zio.dynamodb

import zio.aws.core.config
import zio.aws.dynamodb.DynamoDb
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.dynamodb.model.{ DynamoDbException, IdempotentParameterMismatchException }
import zio.dynamodb.UpdateExpression.Action.SetAction
import zio.dynamodb.UpdateExpression.SetOperand
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.SortKeyExpression.SortKey
import zio.aws.{ dynamodb, netty }
import zio._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression._
import zio.test.Assertion._
import software.amazon.awssdk.regions.Region
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.{ ZSink, ZStream }
import zio.test._
import zio.test.TestAspect._

import java.net.URI
import scala.collection.immutable.{ Map => ScalaMap }

object LiveSpec extends ZIOSpecDefault {

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.configured() >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val testLayer = (dynamoDbLayer >>> DynamoDBExecutor.live)

  private val id       = "id"
  private val first    = "first"
  private val second   = "second"
  private val third    = "third"
  private val name     = "firstName"
  private val number   = "num"
  val secondPrimaryKey = PrimaryKey(id -> second, number -> 2)

  private val notAdam = "notAdam"
  private val avi     = "avi"
  private val avi2    = "avi2"
  private val avi3    = "avi3"
  private val adam    = "adam"
  private val adam2   = "adam2"
  private val adam3   = "adam3"
  private val john    = "john"
  private val john2   = "john2"
  private val john3   = "john3"

  private val stringSortKeyItem = Item(id -> adam, name -> adam)

  private final case class Person(id: String, firstName: String, num: Int)
  private implicit lazy val person: Schema[Person] = DeriveSchema.gen[Person]

  private val aviPerson  = Person(first, avi, 1)
  private val avi2Person = Person(first, avi2, 4)
  private val avi3Person = Person(first, avi3, 7)

  private val aviItem  = Item(id -> first, name -> avi, number -> 1, "map" -> ScalaMap("abc" -> 1, "123" -> 2))
  private val avi2Item = Item(id -> first, name -> avi2, number -> 4)
  private val avi3Item = Item(id -> first, name -> avi3, number -> 7)

  private val adamItem  = Item(id -> second, name -> adam, number -> 2)
  private val adam2Item = Item(id -> second, name -> adam2, number -> 5)
  private val adam3Item = Item(id -> second, name -> adam3, number -> 8, "list" -> List(1, 2, 3))

  private val johnItem  = Item(id -> third, name -> john, number -> 3)
  private val john2Item = Item(id -> third, name -> john2, number -> 6)
  private val john3Item = Item(id -> third, name -> john3, number -> 9)

  private def pk(item: Item): PrimaryKey =
    (item.map.get("id"), item.map.get("num")) match {
      case (Some(id), Some(num)) => PrimaryKey("id" -> id, "num" -> num)
      case _                     => throw new IllegalStateException(s"Both id and num need to present in item $item")
    }

  private def insertData(tableName: String) =
    putItem(tableName, aviItem) *>
      putItem(tableName, avi2Item) *>
      putItem(tableName, avi3Item) *>
      putItem(tableName, adamItem) *>
      putItem(tableName, adam2Item) *>
      putItem(tableName, adam3Item) *>
      putItem(tableName, johnItem) *>
      putItem(tableName, john2Item) *>
      putItem(tableName, john3Item)

  private def defaultTable(tableName: String) =
    createTable(tableName, KeySchema(id, number), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString(id),
      AttributeDefinition.attrDefnNumber(number)
    )

  private def numberTable(tableName: String) =
    createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnNumber(id)
    )

  private def sortKeyStringTable(tableName: String) =
    createTable(tableName, KeySchema(id, name), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString(id),
      AttributeDefinition.attrDefnString(name)
    )

  private def managedTable(tableDefinition: String => CreateTable) =
    ZIO
      .acquireRelease(
        for {
          tableName <- zio.Random.nextUUID.map(_.toString)
          _         <- tableDefinition(tableName).execute
        } yield TableName(tableName)
      )(tName => deleteTable(tName.value).execute.orDie)

  // TODO: Avi - fix problem with inference of this function when splitting suites
  // "a type was inferred to be `Any`; this may indicate a programming error."
  private def withTemporaryTable[R](
    tableDefinition: String => CreateTable,
    f: String => ZIO[DynamoDBExecutor with R, Throwable, TestResult]
  ): ZIO[DynamoDBExecutor with R, Throwable, TestResult] =
    ZIO.scoped[R with DynamoDBExecutor] {
      for {
        table      <- managedTable(tableDefinition)
        testResult <- f(table.value)
      } yield testResult
    }

  private def withDefaultTable(
    f: String => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ) =
    ZIO.scoped {
      managedTable(defaultTable).flatMap { table =>
        for {
          _      <- insertData(table.value).execute
          result <- f(table.value)
        } yield result
      }
    }

  def withDefaultAndNumberTables(
    f: (String, String) => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ) =
    ZIO.scoped {
      for {
        table1 <- managedTable(defaultTable)
        table2 <- managedTable(numberTable)
        _      <- insertData(table1.value).execute
        result <- f(table1.value, table2.value)
      } yield result
    }

  private def assertDynamoDbException(substring: String): Assertion[Any] =
    isSubtype[DynamoDbException](hasMessage(containsString(substring)))

  private val conditionAlwaysTrue = ConditionExpression.Equals(
    ConditionExpression.Operand.ValueOperand(AttributeValue(id)),
    ConditionExpression.Operand.ValueOperand(AttributeValue(id))
  )

  override def spec: Spec[TestEnvironment, Any] = mainSuite

  final case class ExpressionAttrNames(id: String, num: Int, ttl: Option[Long])
  object ExpressionAttrNames {
    implicit val schema = DeriveSchema.gen[ExpressionAttrNames]
    val (id, num, ttl)  = ProjectionExpression.accessors[ExpressionAttrNames]
  }

  val mainSuite: Spec[TestEnvironment, Any] =
    suite("live test")(
      suite("keywords in expression attribute names")(
        suite("using high level api")(
          test("scanAll should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .scanAll[ExpressionAttrNames](tableName)
                .filter(ExpressionAttrNames.ttl.notExists)
              query.execute.flatMap(_.runDrain).exit.map { result =>
                assert(result)(succeeds(isUnit))
              }
            }
          },
          test("queryAll should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .queryAll[ExpressionAttrNames](tableName)
                .whereKey(ExpressionAttrNames.id === "id")
                .filter(ExpressionAttrNames.ttl.notExists)
              query.execute.flatMap(_.runDrain).exit.map { result =>
                assert(result)(succeeds(isUnit))
              }
            }
          },
          test("scanSome should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .scanSome[ExpressionAttrNames](tableName, 1)
                .filter(ExpressionAttrNames.ttl.notExists)

              for {
                result <- query.execute
              } yield assert(result._1)(hasSize(equalTo(1))) && assert(result._1(0))(
                equalTo(ExpressionAttrNames(second, 2, None))
              ) && assert(result._2)(equalTo(Some(PrimaryKey("num" -> 2, "id" -> second))))
            }
          },
          test("querySome should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .querySome[ExpressionAttrNames](tableName, 1)
                .whereKey(PartitionKey(id) === second && SortKey(number) > 0)
                .filter(ExpressionAttrNames.ttl.notExists)

              for {
                result <- query.execute
              } yield assert(result._1)(hasSize(equalTo(1))) && assert(result._1(0))(
                equalTo(ExpressionAttrNames(second, 2, None))
              ) && assert(result._2)(
                equalTo(Some(PrimaryKey("num" -> 2, "id" -> second)))
              )
            }
          },
          test("delete should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .delete[ExpressionAttrNames](tableName, PrimaryKey("id" -> "id", "num" -> 1))
                .where(ExpressionAttrNames.ttl.notExists)
              query.execute.exit.map { result =>
                assert(result)(succeeds(isNone))
              }
            }
          },
          test("put should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .put[ExpressionAttrNames](tableName, ExpressionAttrNames("id", 1, None))
                .where(ExpressionAttrNames.ttl.notExists)
              query.execute.exit.map { result =>
                assert(result)(succeeds(isNone))
              }
            }
          },
          test("update should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .update[ExpressionAttrNames](tableName, PrimaryKey("id" -> "1", "num" -> 1))(
                  ExpressionAttrNames.ttl.set(Some(42L))
                )
                .where(ExpressionAttrNames.ttl.notExists)
              query.execute.exit.map { result =>
                assert(result)(succeeds(isNone))
              }
            }
          }
        ),
        suite("using $ function")(
          test("scanAllItem should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .scanAllItem(tableName)
                .filter($("ttl").notExists)
              query.execute.flatMap(_.runDrain).exit.map { result =>
                assert(result)(succeeds(isUnit))
              }
            }
          },
          test("scanSomeItem should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .scanSomeItem(tableName, 1)
                .filter($("ttl").notExists)
              query.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          },
          test("scanSomeItem should handle keyword in projection") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .scanSomeItem(tableName, 1, $("ttl"))
              query.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          },
          test("queryAllItem should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .queryAllItem(tableName)
                .whereKey($("id") === "id")
                .filter($("ttl").notExists)
              query.execute.flatMap(_.runDrain).exit.map { result =>
                assert(result)(succeeds(isUnit))
              }
            }
          },
          test("querySomeItem should handle keyword") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .querySomeItem(tableName, 1)
                .whereKey($("id") === "id")
                .filter($("ttl").notExists)
              query.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          },
          test("querySomeItem should handle keyword in projection") {
            withDefaultTable { tableName =>
              val query = DynamoDBQuery
                .querySomeItem(tableName, 1, $("ttl"))
                .whereKey($("id") === "id")
              query.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          }
        )
      ),
      suite("basic usage")(
        test("put and get item") {
          withDefaultTable { tableName =>
            for {
              _      <- putItem(tableName, Item(id -> first, "testName" -> "put and get item", number -> 20)).execute
              result <- getItem(tableName, PrimaryKey(id -> first, number -> 20)).execute
            } yield assert(result)(
              equalTo(Some(Item(id -> first, "testName" -> "put and get item", number -> 20)))
            )
          }
        },
        test("put and get item with all attribute types") {
          import zio.dynamodb.ToAttributeValue._
          val allAttributeTypeItem = Item(
            id          -> 0,
            "bin"       -> Chunk.fromArray("abc".getBytes),
            "binSet"    -> Set(Chunk.fromArray("abc".getBytes)),
            "boolean"   -> true,
            "list"      -> List(1, 2, 3),
            "map"       -> ScalaMap(
              "a" -> true,
              "b" -> false,
              "c" -> false
            ),
            "num"       -> 5,
            "numSet"    -> Set(4, 3, 2, 1),
            "null"      -> null,
            "string"    -> "string",
            "stringSet" -> Set("a", "b", "c")
          )
          withTemporaryTable(
            tableName =>
              createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnNumber(id)),
            tableName =>
              for {
                _      <- putItem(tableName, allAttributeTypeItem).execute
                result <- getItem(tableName, PrimaryKey(id -> 0)).execute
              } yield assert(result)(
                equalTo(Some(allAttributeTypeItem))
              )
          )
        },
        test("get item handles keyword in projection expression") {
          withDefaultTable { tableName =>
            for {
              _    <- putItem(tableName, Item(id -> first, number -> 1, "ttl" -> 42L)).execute
              item <- getItem(tableName, pk(aviItem), $(id), $(number), $("ttl")).execute
            } yield assert(item)(equalTo(Some(Item(id -> first, number -> 1, "ttl" -> 42L))))
          }
        },
        test("get into case class") {
          withDefaultTable { tableName =>
            get[Person](tableName, secondPrimaryKey).execute.map(person =>
              assert(person)(equalTo(Right(Person("second", "adam", 2))))
            )
          }
        },
        test("get data from map") {
          withDefaultTable { tableName =>
            for {
              item <- getItem(tableName, pk(aviItem), $(id), $(number), $("map.abc")).execute
            } yield assert(item)(equalTo(Some(Item(id -> first, number -> 1, "map" -> ScalaMap("abc" -> 1)))))
          }
        },
        test("get nonexistant returns empty") {
          withDefaultTable { tableName =>
            getItem(tableName, PrimaryKey(id -> "nowhere", number -> 1000)).execute.map(item => assert(item)(isNone))
          }
        },
        test("empty set not written") {
          withDefaultTable { tableName =>
            val setItem = Item(number -> 100, id -> "set", "emptySet" -> Set.empty[Int])
            for {
              _ <- putItem(tableName, setItem).execute
              a <- getItem(tableName, PrimaryKey(number -> 100, id -> "set")).execute
            } yield assert(a.flatMap(_.map.get("emptySet")))(isNone)
          }
        },
        test("delete item with false where clause") {
          withDefaultTable { tableName =>
            val deleteItem = DeleteItem(
              key = pk(avi3Item),
              tableName = TableName(tableName)
            ).where($("firstName").beginsWith("noOne"))
            assertZIO(deleteItem.execute.exit)(fails(assertDynamoDbException("The conditional request failed")))
          }
        },
        test("put item with false where clause") {
          withDefaultTable { tableName =>
            val putItem = PutItem(
              tableName = TableName(tableName),
              item = Item(id -> "nothing", number -> 900)
            ).where($("id").beginsWith("false"))

            assertZIO(putItem.execute.exit)(fails(assertDynamoDbException("The conditional request failed")))
          }
        },
        test("batch get item") {
          withDefaultTable { tableName =>
            val getItems = BatchGetItem().addAll(
              GetItem(TableName(tableName), pk(avi3Item)),
              GetItem(TableName(tableName), pk(adam2Item))
            )
            for {
              a <- getItems.execute
            } yield assert(a)(
              equalTo(
                BatchGetItem.Response(
                  responses = MapOfSet.apply(
                    ScalaMap[TableName, Set[Item]](
                      TableName(tableName) -> Set(avi3Item, adam2Item)
                    )
                  )
                )
              )
            )
          }
        }
      ),
      suite("scan tables")(
        test("scan table") {
          withDefaultTable { tableName =>
            for {
              stream <- scanAllItem(tableName).execute
              chunk  <- stream.runCollect
            } yield assert(chunk)(
              equalTo(
                Chunk(adamItem, adam2Item, adam3Item, johnItem, john2Item, john3Item, aviItem, avi2Item, avi3Item)
              )
            )
          }
        },
        test("scan table with filter") {
          withDefaultTable { tableName =>
            for {
              stream <- scanAll[Person](tableName)
                          .filter(
                            ConditionExpression.Equals(
                              ConditionExpression.Operand.ProjectionExpressionOperand($(id)),
                              ConditionExpression.Operand.ValueOperand(AttributeValue(first))
                            )
                          )
                          .execute
              chunk  <- stream.runCollect
            } yield assert(chunk)(equalTo(Chunk(aviPerson, avi2Person, avi3Person)))
          }
        },
        test("parallel scan all item") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _      <- batchWriteFromStream(ZStream.fromIterable(1 to 10000).map(i => Item(id -> i))) { item =>
                            putItem(tableName, item)
                          }.runDrain
                stream <- scanAllItem(tableName).parallel(8).execute
                count  <- stream.runFold(0) { case (count, _) => count + 1 }
              } yield assert(count)(equalTo(10000))
          )
        },
        test("parallel scan all typed") {
          withTemporaryTable(
            defaultTable,
            tableName =>
              for {
                _      <-
                  batchWriteFromStream(
                    ZStream.fromIterable(1 to 10000).map(i => Item(id -> i.toString, number -> i, name -> i.toString))
                  ) { item =>
                    putItem(tableName, item)
                  }.runDrain
                stream <- scanAll[Person](tableName).parallel(8).execute
                count  <- stream.runFold(0) { case (count, _) => count + 1 }
              } yield assert(count)(equalTo(10000))
          )
        }
      ),
      suite("query tables")(
        test("query all projection expressions should handle keyword") {
          withDefaultTable { tableName =>
            val query =
              queryAllItem(tableName, $(name), $("ttl")).whereKey(
                PartitionKey(id) === first && SortKey(number) > 0
              )

            query.execute.flatMap(_.runDrain).map { _ =>
              assertCompletes
            }
          }
        },
        test("query some projection expressions should handle keyword") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 10, $(name), $("ttl"))
                         .whereKey(PartitionKey(id) === first && SortKey(number) > 0)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi), Item(name -> avi2), Item(name -> avi3)))
            )
          }
        },
        test("query less than") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 10, $(name))
                         .whereKey(PartitionKey(id) === first && SortKey(number) < 2)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi)))
            )
          }
        },
        test("query table greater than") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 10, $(name))
                         .whereKey(PartitionKey(id) === first && SortKey(number) > 0)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi), Item(name -> avi2), Item(name -> avi3)))
            )
          }
        },
        // Note notEqual is NOT a valid SortKey condition: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestSyntax
        test("query table greater than or equal") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 10, $(name))
                         .whereKey(PartitionKey(id) === first && SortKey(number) >= 4)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi2), Item(name -> avi3)))
            )
          }
        },
        test("query table less than or equal") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 10, $(name))
                         .whereKey(PartitionKey(id) === first && SortKey(number) <= 4)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi), Item(name -> avi2)))
            )
          }
        },
        test("empty query result returns empty chunk") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 10, $(name))
                         .whereKey(PartitionKey(id) === "nowhere" && SortKey(number) > 0)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(isEmpty)
          }
        },
        test("query with limit > 0 and limit < matching items count") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 1, $(name))
                         .whereKey(PartitionKey(id) === first)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi))))
          }
        },
        test("query with limit == matching items count") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 3, $(name))
                         .whereKey(PartitionKey(id) === first)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi), Item(name -> avi2), Item(name -> avi3))))
          }
        },
        test("query with limit > matching items count") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 4, $(name))
                         .whereKey(PartitionKey(id) === first)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi), Item(name -> avi2), Item(name -> avi3))))
          }
        },
        test("query starting from StartKey") {
          withDefaultTable { tableName =>
            for {
              startKey <- querySomeItem(tableName, 2, $(id), $(number))
                            .whereKey(PartitionKey(id) === first)
                            .execute
                            .map(_._2)
              chunk    <- querySomeItem(tableName, 5, $(name))
                            .whereKey(PartitionKey(id) === first)
                            .startKey(startKey)
                            .execute
                            .map(_._1)
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi3))))
          }
        },
        test("queryAll") {
          withDefaultTable { tableName =>
            for {
              stream <- queryAllItem(tableName)
                          .whereKey(PartitionKey(id) === second)
                          .execute
              chunk  <- stream.run(ZSink.collectAll[Item])
            } yield assert(chunk)(
              equalTo(
                Chunk(adamItem, adam2Item, adam3Item)
              )
            )
          }
        },
        suite("SortKeyCondition")(
          test("SortKeyCondition between") {
            withDefaultTable { tableName =>
              for {
                chunk <- querySomeItem(tableName, 10, $(name))
                           .whereKey(PartitionKey(id) === first && SortKey(number).between(3, 8))
                           .execute
                           .map(_._1)
              } yield assert(chunk)(
                equalTo(Chunk(Item(name -> avi2), Item(name -> avi3)))
              )
            }
          },
          test("SortKeyCondition startsWtih") {
            withTemporaryTable(
              sortKeyStringTable,
              tableName =>
                for {
                  _     <- putItem(tableName, stringSortKeyItem).execute
                  chunk <- querySomeItem(tableName, 10)
                             .whereKey(PartitionKey(id) === adam && SortKey(name).beginsWith("ad"))
                             .execute
                             .map(_._1)
                } yield assert(chunk)(equalTo(Chunk(stringSortKeyItem)))
            )
          }
        )
      ),
      suite("update items")(
        suite("set actions")(
          test("update name") {
            withDefaultTable { tableName =>
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).set(notAdam)).execute
                updated         <- getItem(
                                     tableName,
                                     pk(adamItem)
                                   ).execute
              } yield assert(updated)(equalTo(Some(Item(name -> notAdam, id -> second, number -> 2)))) && assert(
                updatedResponse
              )(isNone)
            }
          },
          test("update name return updated old") {
            withDefaultTable { tableName =>
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).set(notAdam))
                                     .returns(ReturnValues.UpdatedOld)
                                     .execute
                updated         <- getItem(
                                     tableName,
                                     pk(adamItem)
                                   ).execute
              } yield assert(updated)(equalTo(Some(Item(name -> notAdam, id -> second, number -> 2)))) &&
                assert(updatedResponse)(equalTo(Some(Item(name -> adam))))
            }
          },
          test("update name return all old") {
            withDefaultTable { tableName =>
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).set(notAdam))
                                     .returns(ReturnValues.AllOld)
                                     .execute
                updated         <- getItem(
                                     tableName,
                                     pk(adamItem)
                                   ).execute
              } yield assert(updated)(equalTo(Some(Item(name -> notAdam, id -> second, number -> 2)))) &&
                assert(updatedResponse)(equalTo(Some(adamItem)))
            }
          },
          test("update name return all new") {
            withDefaultTable { tableName =>
              val updatedItem = Some(Item(name -> notAdam, id -> second, number -> 2))
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).set(notAdam))
                                     .returns(ReturnValues.AllNew)
                                     .execute
                updated         <- getItem(
                                     tableName,
                                     pk(adamItem)
                                   ).execute
              } yield assert(updated)(equalTo(updatedItem)) &&
                assert(updatedResponse)(equalTo(updatedItem))
            }
          },
          test("update name return updated new") {
            withDefaultTable { tableName =>
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).set(notAdam))
                                     .returns(ReturnValues.UpdatedNew)
                                     .execute
                updated         <- getItem(
                                     tableName,
                                     pk(adamItem)
                                   ).execute
              } yield assert(updated)(equalTo(Some(Item(name -> notAdam, id -> second, number -> 2)))) &&
                assert(updatedResponse)(equalTo(Some(Item(name -> notAdam))))
            }
          },
          test("insert item into list") {
            withDefaultTable { tableName =>
              for {
                _       <- updateItem(tableName, pk(adamItem))($("listThing").set(List(1))).execute
                _       <- updateItem(tableName, pk(adamItem))($("listThing[1]").set(2)).execute
                updated <- getItem(
                             tableName,
                             pk(adamItem)
                           ).execute
              } yield assert(updated)(
                equalTo(Some(Item(id -> second, number -> 2, name -> adam, "listThing" -> List(1, 2))))
              )
            }
          },
          test("append to list") {
            withDefaultTable { tableName =>
              for {
                _       <- updateItem(tableName, pk(adamItem))($("listThing").set(List(1))).execute
                _       <- updateItem(tableName, pk(adamItem))($("listThing").appendList(Chunk(2, 3, 4))).execute
                updated <- getItem(tableName, pk(adamItem)).execute
              } yield assert(
                updated.map(a =>
                  a.get("listThing")(
                    FromAttributeValue.iterableFromAttributeValue(FromAttributeValue.intFromAttributeValue)
                  )
                )
              )(equalTo(Some(Right(List(1, 2, 3, 4)))))
            }
          },
          test("prepend to list") {
            withDefaultTable { tableName =>
              for {
                _       <- updateItem(tableName, pk(adamItem))($("listThing").set(List(1))).execute
                _       <- updateItem(tableName, pk(adamItem))($("listThing").prependList(Chunk(-1, 0))).execute
                updated <- getItem(tableName, pk(adamItem)).execute
              } yield assert(
                updated.map(a =>
                  a.get("listThing")(
                    FromAttributeValue.iterableFromAttributeValue(FromAttributeValue.intFromAttributeValue)
                  )
                )
              )(equalTo(Some(Right(List(-1, 0, 1)))))
            }
          },
          test("set an Item Attribute") {
            withDefaultTable { tableName =>
              for {
                _       <- updateItem(tableName, pk(adamItem))($(name).set($(id))).execute
                updated <- getItem(
                             tableName,
                             pk(adamItem)
                           ).execute
              } yield assert(updated)(
                equalTo(Some(Item(id -> second, number -> 2, name -> second)))
              )
            }
          },
          suite("if not exists")(
            test("field does not exist with a projection expression on both sides") {
              withTemporaryTable(
                numberTable,
                tableName =>
                  for {
                    _       <- putItem(tableName, Item(id -> 1)).execute
                    _       <- updateItem(tableName, PrimaryKey(id -> 1))($(number).setIfNotExists($(number), 4))
                                 .where(
                                   ConditionExpression.NotEqual(
                                     ConditionExpression.Operand.ProjectionExpressionOperand($(id)),
                                     ConditionExpression.Operand.ValueOperand(AttributeValue(id))
                                   )
                                 )
                                 .execute
                    updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
                  } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> 4))))
              )
            },
            test("field does not exist using projection expression on LHS and a value on RHS") {
              withTemporaryTable(
                numberTable,
                tableName =>
                  for {
                    _       <- putItem(tableName, Item(id -> 1)).execute
                    _       <- updateItem(tableName, PrimaryKey(id -> 1))($(number).setIfNotExists(4))
                                 .where(
                                   ConditionExpression.NotEqual(
                                     ConditionExpression.Operand.ProjectionExpressionOperand($(id)),
                                     ConditionExpression.Operand.ValueOperand(AttributeValue(id))
                                   )
                                 )
                                 .execute
                    updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
                  } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> 4))))
              )
            },
            test("does not update if field does exist") {
              withTemporaryTable(
                numberTable,
                tableName =>
                  for {
                    _       <- putItem(tableName, Item(id -> 1, number -> 0)).execute
                    _       <- updateItem(tableName, PrimaryKey(id -> 1))($(number).setIfNotExists($(number), 4)).execute
                    updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
                  } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> 0))))
              )
            }
          )
        ),
        test("remove field") {
          withDefaultTable { tableName =>
            for {
              _       <- updateItem(tableName, pk(adamItem))($(name).remove).execute
              updated <- getItem(
                           tableName,
                           pk(adamItem)
                         ).execute
            } yield assert(updated)(equalTo(Some(Item(id -> second, number -> 2))))
          }
        },
        test("remove an element from a list addressed by an index") {
          withDefaultTable { tableName =>
            val key = PrimaryKey(id -> second, number -> 8)
            for {
              _       <- updateItem(tableName, key)($("list[1]").remove).execute
              updated <- getItem(
                           tableName,
                           key
                         ).execute
            } yield assert(updated)(
              equalTo(Some(Item(id -> second, number -> 8, name -> adam3, "list" -> List(1, 3))))
            )
          }
        },
        test("remove an element from list passing index to remove") {
          withDefaultTable { tableName =>
            val key = PrimaryKey(id -> second, number -> 8)
            for {
              _       <- updateItem(tableName, key)($("list").remove(1)).execute
              updated <- getItem(
                           tableName,
                           key
                         ).execute
            } yield assert(updated)(
              equalTo(Some(Item(id -> second, number -> 8, name -> adam3, "list" -> List(1, 3))))
            )
          }
        },
        test("add a set to set") {
          withTemporaryTable(
            tableName =>
              createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnNumber(id)),
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "set" -> Set(1, 2, 3))).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))($("set").add(Set(4))).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "set" -> Set(1, 2, 3, 4)))))
          )
        },
        test("delete elements from a set") {
          withTemporaryTable(
            tableName =>
              createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnNumber(id)),
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "set" -> Set(1, 2, 3))).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))($("set").deleteFromSet(Set(3))).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "set" -> Set(1, 2)))))
          )
        },
        test("`in` using a range of set when field is a set") {
          withTemporaryTable(
            tableName =>
              createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnNumber(id)),
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "set" -> Set(1, 2))).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))($("set").addSet(Set(3)))
                             .where(
                               ConditionExpression.Operand
                                 .ProjectionExpressionOperand($("set"))
                                 .in(Set(AttributeValue(Set(1, 2))))
                             )
                             .execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "set" -> Set(1, 2, 3)))))
          )
        },
        test("`in` using a range of scalar when field is a scalar") {
          withTemporaryTable(
            tableName =>
              createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnNumber(id)),
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "age" -> 21)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))($("age").add(1))
                             .where(
                               ConditionExpression.Operand
                                 .ProjectionExpressionOperand($("age"))
                                 .in(Set(AttributeValue(21), AttributeValue(22)))
                             )
                             .execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "age" -> 22))))
          )
        },
        test("add item to a numeric attribute") {
          withTemporaryTable(
            tableName =>
              createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnNumber(id)),
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "num" -> 42)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))($("num").add(1)).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "num" -> 43))))
          )
        },
        test("add number using add action") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, number -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))($(number).add(5)).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> 5))))
          )
        },
        test("add number using set action with a value") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, number -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))(
                             SetAction(
                               $(number),
                               SetOperand.PathOperand($(number)) + SetOperand.ValueOperand(
                                 AttributeValue.Number(5)
                               )
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> 5))))
          )
        },
        test("add number using set action with a projection expression") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, number -> 2)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))(
                             SetAction(
                               $(number),
                               SetOperand.PathOperand($(number)) + SetOperand.PathOperand($(number))
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> 4))))
          )
        },
        test("subtract number using a value") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, number -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))(
                             SetAction(
                               $(number),
                               SetOperand.PathOperand($(number)) - SetOperand.ValueOperand(
                                 AttributeValue.Number(2)
                               )
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> -2))))
          )
        }
      ),
      test("subtract number using a projection expression") {
        withTemporaryTable(
          numberTable,
          tableName =>
            for {
              _       <- putItem(tableName, Item(id -> 1, number -> 4)).execute
              _       <- updateItem(tableName, PrimaryKey(id -> 1))(
                           SetAction(
                             $(number),
                             SetOperand.PathOperand($(number)) - SetOperand.PathOperand($(number))
                           )
                         ).execute
              updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
            } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> 0))))
        )
      },
      suite("transactions")(
        suite("transact write items")(
          test("transact put item should handle key word") {
            withDefaultTable { tableName =>
              val p = put[ExpressionAttrNames](
                tableName = tableName,
                ExpressionAttrNames("id", 10, None)
              ).where(ExpressionAttrNames.ttl.notExists)
              p.transaction.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          },
          test("put item") {
            withDefaultTable { tableName =>
              val putItem = PutItem(
                item = Item(id -> first, name -> avi3, number -> 10),
                tableName = TableName(tableName)
              )
              for {
                _ <- putItem.transaction.execute
                written <- getItem(tableName, PrimaryKey(id -> first, number -> 10)).execute
              } yield assert(written)(isSome(equalTo(putItem.item)))
            }
          },
          test("conditionCheck should handle keyword") {
            withDefaultTable { tableName =>
              val cc    = conditionCheck(
                tableName,
                PrimaryKey("id" -> "id", "num" -> 1)
              )(ExpressionAttrNames.ttl.notExists)
              val p     = put[ExpressionAttrNames](tableName, ExpressionAttrNames("id", 2, None))
              val query = cc.zip(p).transaction
              query.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          },
          test("condition check succeeds") {
            withDefaultTable { tableName =>
              val conditionCheck = ConditionCheck(
                primaryKey = pk(avi3Item),
                tableName = TableName(tableName),
                conditionExpression = conditionAlwaysTrue
              )
              val putItem        = PutItem(
                item = Item(id -> first, name -> avi3, number -> 10),
                tableName = TableName(tableName)
              )

              for {
                _       <- conditionCheck.zip(putItem).transaction.execute
                written <- getItem(tableName, PrimaryKey(id -> first, number -> 10)).execute
              } yield assert(written)(isSome)
            }
          },
          test("condition check fails because 'id' != 'first'") {
            withDefaultTable { tableName =>
              val conditionCheck = ConditionCheck(
                primaryKey = pk(avi3Item),
                tableName = TableName(tableName),
                conditionExpression = ConditionExpression.Equals(
                  ConditionExpression.Operand.ValueOperand(AttributeValue(id)),
                  ConditionExpression.Operand.ValueOperand(AttributeValue(first))
                )
              )
              val putItem        = PutItem(
                item = Item(id -> first, name -> avi3, number -> 10),
                tableName = TableName(tableName)
              )

              assertZIO(
                conditionCheck.zip(putItem).transaction.execute.exit
              )(
                fails(
                  assertDynamoDbException(
                    "Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed, None]"
                  )
                )
              )
            }
          },
          test("delete item handles keyword") {
            withDefaultTable { tableName =>
              val d = delete[ExpressionAttrNames](
                tableName = tableName,
                key = pk(avi3Item)
              ).where(ExpressionAttrNames.ttl.notExists)
              d.transaction.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          },
          test("delete item") {
            withDefaultTable { tableName =>
              val deleteItem = DeleteItem(
                key = pk(avi3Item),
                tableName = TableName(tableName)
              )
              for {
                _       <- deleteItem.transaction.execute
                written <- getItem(tableName, PrimaryKey(id -> first, number -> 7)).execute
              } yield assert(written)(isNone)
            }
          },
          test("transact update item should handle keyword") {
            withDefaultTable { tableName =>
              val u = update[ExpressionAttrNames](
                tableName = tableName,
                key = pk(avi3Item)
              )(ExpressionAttrNames.ttl.set(None)).where(ExpressionAttrNames.ttl.notExists)
              u.transaction.execute.exit.map { result =>
                assert(result.isSuccess)(isTrue)
              }
            }
          },
          test("update item") {
            withDefaultTable { tableName =>
              val updateItem = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).set(notAdam))
              )
              for {
                _       <- updateItem.transaction.execute
                written <- get[Person](tableName, pk(avi3Item)).execute
              } yield assert(written)(isRight(equalTo(Person(first, notAdam, 7))))
            }
          },
          test("all transaction types at once") {
            withDefaultTable { tableName =>
              val putItem        = PutItem(
                item = Item(id -> first, name -> avi3, number -> 10),
                tableName = TableName(tableName)
              )
              val conditionCheck = ConditionCheck(
                primaryKey = pk(aviItem),
                tableName = TableName(tableName),
                conditionExpression = conditionAlwaysTrue
              )
              val updateItem     = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).set(notAdam))
              )
              val deleteItem     = DeleteItem(
                key = pk(avi2Item),
                tableName = TableName(tableName)
              )

              for {
                _       <- (putItem zip conditionCheck zip updateItem zip deleteItem).transaction.execute
                put     <- get[Person](tableName, Item(id -> first, number -> 10)).execute
                deleted <- get[Person](tableName, Item(id -> first, number -> 4)).execute
                updated <- get[Person](tableName, Item(id -> first, number -> 7)).execute
              } yield assert(put)(isRight(equalTo(Person(first, avi3, 10)))) &&
                assert(deleted)(isLeft) &&
                assert(updated)(isRight(equalTo(Person(first, notAdam, 7))))
            }
          },
          test("two updates to same item fails") {
            withDefaultTable { tableName =>
              val updateItem1 = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).set("abc"))
              )

              val updateItem2 = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).set("shouldFail"))
              )

              assertZIO(updateItem1.zip(updateItem2).transaction.execute.exit)(
                fails(assertDynamoDbException("Transaction request cannot include multiple operations on one item"))
              )
            }
          },
          test("repeated client request token with different transaction fails") {
            withDefaultTable { tableName =>
              val updateItem = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).set(notAdam))
              ).transaction.withClientRequestToken("test-token")

              val updateItem2 = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).set("BOOOOOOO"))
              ).transaction.withClientRequestToken("test-token")

              val program = for {
                _ <- updateItem.execute
                _ <- updateItem2.execute
              } yield ()

              assertZIO(program.exit)(
                fails(isSubtype[IdempotentParameterMismatchException](Assertion.anything))
              )
            }
          }
        ),
        suite("transact get items")(
          test("basic transact get items") {
            withDefaultTable { tableName =>
              val getItems =
                GetItem(TableName(tableName), pk(avi3Item))
                  .zip(GetItem(TableName(tableName), pk(adam2Item)))
              for {
                a <- getItems.transaction.execute
              } yield assert(a)(equalTo((Some(avi3Item), Some(adam2Item))))
            }
          },
          test("basic batch get item transaction") {
            withDefaultTable { tableName =>
              val getItems = BatchGetItem().addAll(
                GetItem(TableName(tableName), pk(avi3Item)),
                GetItem(TableName(tableName), pk(adam2Item))
              )
              for {
                a <- getItems.transaction.execute
              } yield assert(a)(
                equalTo(
                  BatchGetItem.Response(
                    responses = MapOfSet.apply(
                      ScalaMap[TableName, Set[Item]](
                        TableName(tableName) -> Set(avi3Item, adam2Item)
                      )
                    )
                  )
                )
              )
            }
          },
          test("missing item does not result in failure") {
            withDefaultTable { tableName =>
              val getItems =
                GetItem(TableName(tableName), Item(id -> first, number -> 1000))
                  .zip(GetItem(TableName(tableName), pk(adam2Item)))
              for {
                a <- getItems.transaction.execute
              } yield assert(a)(equalTo((None, Some(adam2Item))))
            }
          },
          test("missing item in other table") {
            withDefaultAndNumberTables { (tableName, secondTable) =>
              val getItems = BatchGetItem().addAll(
                GetItem(TableName(tableName), pk(avi3Item)),
                GetItem(TableName(tableName), pk(adam2Item)),
                GetItem(TableName(secondTable), Item(id -> 5))
              )
              for {
                a <- getItems.transaction.execute
              } yield assert(a)(
                equalTo(
                  BatchGetItem.Response(
                    responses = MapOfSet.apply(
                      ScalaMap[TableName, Set[Item]](
                        TableName(tableName) -> Set(avi3Item, adam2Item)
                      )
                    )
                  )
                )
              )
            }
          }
        )
      )
    )
      .provideSomeLayerShared[TestEnvironment](
        testLayer.orDie
      ) @@ nondeterministic
}
