package zio.dynamodb

import zio.aws.core.config
import zio.aws.dynamodb.DynamoDb
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
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
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (netty.NettyHttpClient.default ++ awsConfig) >>> config.AwsConfig.configured() >>> dynamodb.DynamoDb.customized {
      builder =>
        builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val testLayer =
    (dynamoDbLayer >>> DynamoDBExecutor.live) ++ LocalDdbServer.inMemoryLayer

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

  private val aviItem  = Item(id -> first, name -> avi, number -> 1, "mapp" -> ScalaMap("abc" -> 1, "123" -> 2))
  private val avi2Item = Item(id -> first, name -> avi2, number -> 4)
  private val avi3Item = Item(id -> first, name -> avi3, number -> 7)

  private val adamItem  = Item(id -> second, name -> adam, number -> 2)
  private val adam2Item = Item(id -> second, name -> adam2, number -> 5)
  private val adam3Item = Item(id -> second, name -> adam3, number -> 8, "listt" -> List(1, 2, 3))

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

  private def assertDynamoDbException(substring: String): Assertion[Any] =
    isSubtype[DynamoDbException](hasMessage(containsString(substring)))

  private val conditionAlwaysTrue = ConditionExpression.Equals(
    ConditionExpression.Operand.ValueOperand(AttributeValue(id)),
    ConditionExpression.Operand.ValueOperand(AttributeValue(id))
  )

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("live test")(
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
            "mapp"      -> ScalaMap(
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
              item <- getItem(tableName, pk(aviItem), $(id), $(number), $("mapp.abc")).execute
            } yield assert(item)(equalTo(Some(Item(id -> first, number -> 1, "mapp" -> ScalaMap("abc" -> 1)))))
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
        test("query table not equal") {
          withDefaultTable { tableName =>
            for {
              chunk <- querySomeItem(tableName, 10, $(name))
                         .whereKey(PartitionKey(id) === first && SortKey(number) <> 1)
                         .execute
                         .map(_._1)
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi2), Item(name -> avi3)))
            )
          }
        } @@ ignore, // I'm not sure notEqual is a valid SortKey condition: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestSyntax
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
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam)).execute
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
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam))
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
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam))
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
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam))
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
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam))
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
                _       <- updateItem(tableName, pk(adamItem))($("listThing").setValue(List(1))).execute
                _       <- updateItem(tableName, pk(adamItem))($("listThing[1]").setValue(2)).execute
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
                _       <- updateItem(tableName, pk(adamItem))($("listThing").setValue(List(1))).execute
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
                _       <- updateItem(tableName, pk(adamItem))($("listThing").setValue(List(1))).execute
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
            test("field does not exist") {
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
        test("remove item from list") {
          withDefaultTable { tableName =>
            val key = PrimaryKey(id -> second, number -> 8)
            for {
              _       <- updateItem(tableName, key)($("listt[1]").remove).execute
              updated <- getItem(
                           tableName,
                           key
                         ).execute
            } yield assert(updated)(
              equalTo(Some(Item(id -> second, number -> 8, name -> adam3, "listt" -> List(1, 3))))
            )
          }
        },
        test("add item to set") {
          withTemporaryTable(
            tableName =>
              createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnNumber(id)),
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "sett" -> Set(1, 2, 3))).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))($("sett").add(Set(4))).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "sett" -> Set(1, 2, 3, 4)))))
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
        test("add number using set action") {
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
        test("subtract number") {
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
      suite("transactions")(
        suite("transact write items")(
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

              assertM(
                conditionCheck.zip(putItem).transaction.execute.exit
              )(fails(assertDynamoDbException("ConditionalCheckFailed")))
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
          test("update item") {
            withDefaultTable { tableName =>
              val updateItem = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).setValue(notAdam))
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
                updateExpression = UpdateExpression($(name).setValue(notAdam))
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
                updateExpression = UpdateExpression($(name).setValue("abc"))
              )

              val updateItem2 = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).setValue("shouldFail"))
              )

              assertM(updateItem1.zip(updateItem2).transaction.execute.exit)(
                fails(assertDynamoDbException("Transaction request cannot include multiple operations on one item"))
              )
            }
          },
          test("repeated client request token with different transaction fails") {
            withDefaultTable { tableName =>
              val updateItem = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).setValue(notAdam))
              ).transaction.withClientRequestToken("test-token")

              val updateItem2 = UpdateItem(
                key = pk(avi3Item),
                tableName = TableName(tableName),
                updateExpression = UpdateExpression($(name).setValue("BOOOOOOO"))
              ).transaction.withClientRequestToken("test-token")

              val program = for {
                _ <- updateItem.execute
                _ <- updateItem2.execute
              } yield ()

              assertM(program.exit)(
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
            withDefaultTable { tableName =>
              val secondTable = numberTable("some-table")
              val getItems    = BatchGetItem().addAll(
                GetItem(TableName(tableName), pk(avi3Item)),
                GetItem(TableName(tableName), pk(adam2Item)),
                GetItem(TableName("some-table"), Item(id -> 5))
              )
              for {
                _ <- secondTable.execute
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
