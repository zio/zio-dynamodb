package zio.dynamodb

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import zio.blocking.Blocking
import zio.dynamodb.UpdateExpression.Action.SetAction
import zio.dynamodb.UpdateExpression.SetOperand
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.SortKeyExpression.SortKey
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio._
import zio.clock.Clock
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression._
import zio.test.Assertion._
import zio.test.environment._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.{ DynamoDbException, IdempotentParameterMismatchException }
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.{ ZSink, ZStream }
import zio.test._
import zio.test.TestAspect._

import java.net.URI
import scala.collection.immutable.{ Map => ScalaMap }

object LiveSpec extends DefaultRunnableSpec {

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (http4s.default ++ awsConfig) >>> config.configured() >>> dynamodb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val layer =
    ((dynamoDbLayer ++ ZLayer
      .identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live) ++ (ZLayer
      .identity[Has[Blocking.Service]] >>> LocalDdbServer.inMemoryLayer)

  private val id      = "id"
  private val first   = "first"
  private val second  = "second"
  private val third   = "third"
  private val name    = "firstName"
  private val number  = "num"
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
    ZManaged
      .make(
        for {
          tableName <- random.nextUUID.map(_.toString)
          _         <- tableDefinition(tableName).execute
        } yield TableName(tableName)
      )(tName => deleteTable(tName.value).execute.orDie)

  private def withTemporaryTable[R](
    tableDefinition: String => CreateTable,
    f: String => ZIO[Has[DynamoDBExecutor] with R, Throwable, TestResult]
  ) =
    managedTable(tableDefinition).use(table => f(table.value))

  private def withDefaultTable(
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    managedTable(defaultTable).use { table =>
      for {
        _      <- insertData(table.value).execute
        result <- f(table.value)
      } yield result
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
        testM("put and get item") {
          withDefaultTable { tableName =>
            for {
              _      <- putItem(tableName, Item(id -> first, "testName" -> "put and get item", number -> 20)).execute
              result <- getItem(tableName, PrimaryKey(id -> first, number -> 20)).execute
            } yield assert(result)(
              equalTo(Some(Item(id -> first, "testName" -> "put and get item", number -> 20)))
            )
          }
        },
        testM("put and get item with all attribute types") {
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
        testM("get into case class") {
          withDefaultTable { tableName =>
            get[Person](tableName, pk(adamItem)).execute.map(person =>
              assert(person)(equalTo(Right(Person("second", "adam", 2))))
            )
          }
        },
        testM("get data from map") {
          withDefaultTable { tableName =>
            for {
              item <- getItem(tableName, pk(aviItem), $(id), $(number), $("mapp.abc")).execute
            } yield assert(item)(equalTo(Some(Item(id -> first, number -> 1, "mapp" -> ScalaMap("abc" -> 1)))))
          }
        },
        testM("get nonexistant returns empty") {
          withDefaultTable { tableName =>
            getItem(tableName, PrimaryKey(id -> "nowhere", number -> 1000)).execute.map(item => assert(item)(isNone))
          }
        },
        testM("batch get item") {
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
        testM("scan table") {
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
        testM("scan table with filter") {
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
        testM("parallel scan all item") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _      <- batchWriteFromStream(ZStream.fromIterable(1 to 10000).map(i => Item(id -> i))) { item =>
                            putItem(tableName, item)
                          }.runDrain
                stream <- scanAllItem(tableName).parallel(8).execute
                count  <- stream.fold(0) { case (count, _) => count + 1 }
              } yield assert(count)(equalTo(10000))
          )
        },
        testM("parallel scan all typed") {
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
                count  <- stream.fold(0) { case (count, _) => count + 1 }
              } yield assert(count)(equalTo(10000))
          )
        }
      ),
      suite("query tables")(
        testM("query less than") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === first && SortKey(number) < 2)
                              .execute
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi)))
            )
          }
        },
        testM("query table greater than") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === first && SortKey(number) > 0)
                              .execute

            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi), Item(name -> avi2), Item(name -> avi3)))
            )
          }
        },
        testM("query table not equal") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === first && SortKey(number) <> 1)
                              .execute
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi2), Item(name -> avi3)))
            )
          }
        } @@ ignore, // I'm not sure notEqual is a valid SortKey condition: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestSyntax
        testM("query table greater than or equal") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === first && SortKey(number) >= 4)
                              .execute
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi2), Item(name -> avi3)))
            )
          }
        },
        testM("query table less than or equal") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === first && SortKey(number) <= 4)
                              .execute
            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi), Item(name -> avi2)))
            )
          }
        },
        testM("empty query result returns empty chunk") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === "nowhere" && SortKey(number) > 0)
                              .execute
            } yield assert(chunk)(isEmpty)
          }
        },
        testM("query with limit == 0") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 0, $(name))
                              .whereKey(PartitionKey(id) === first)
                              .execute
            } yield assert(chunk)(equalTo(Chunk.empty))
          }
        },
        testM("query with limit > 0 and limit < matching items count") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 1, $(name))
                              .whereKey(PartitionKey(id) === first)
                              .execute
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi))))
          }
        },
        testM("query with limit == matching items count") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 3, $(name))
                              .whereKey(PartitionKey(id) === first)
                              .execute
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi), Item(name -> avi2), Item(name -> avi3))))
          }
        },
        testM("query with limit > matching items count") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 4, $(name))
                              .whereKey(PartitionKey(id) === first)
                              .execute
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi), Item(name -> avi2), Item(name -> avi3))))
          }
        },
        testM("query starting from StartKey") {
          withDefaultTable { tableName =>
            for {
              (_, startKey) <- querySomeItem(tableName, 2, $(id), $(number))
                                 .whereKey(PartitionKey(id) === first)
                                 .execute
              (chunk, _)    <- querySomeItem(tableName, 5, $(name))
                                 .whereKey(PartitionKey(id) === first)
                                 .startKey(startKey)
                                 .execute
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi3))))
          }
        },
        testM("queryAll") {
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
          testM("SortKeyCondition between") {
            withDefaultTable { tableName =>
              for {
                (chunk, _) <- querySomeItem(tableName, 10, $(name))
                                .whereKey(PartitionKey(id) === first && SortKey(number).between(3, 8))
                                .execute
              } yield assert(chunk)(
                equalTo(Chunk(Item(name -> avi2), Item(name -> avi3)))
              )
            }
          },
          testM("SortKeyCondition startsWtih") {
            withTemporaryTable(
              sortKeyStringTable,
              tableName =>
                for {
                  _          <- putItem(tableName, stringSortKeyItem).execute
                  (chunk, _) <- querySomeItem(tableName, 10)
                                  .whereKey(PartitionKey(id) === adam && SortKey(name).beginsWith("ad"))
                                  .execute
                } yield assert(chunk)(equalTo(Chunk(stringSortKeyItem)))
            )
          }
        )
      ),
      suite("update items")(
        suite("set actions")(
          testM("update name") {
            withDefaultTable { tableName =>
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam)).execute
                updated         <- getItem(tableName, pk(adamItem)).execute
              } yield assert(updated)(equalTo(Some(Item(name -> notAdam, id -> second, number -> 2)))) && assert(
                updatedResponse
              )(isNone)
            }
          },
          testM("update name return updated old") {
            withDefaultTable { tableName =>
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam))
                                     .returns(ReturnValues.UpdatedOld)
                                     .execute
                updated         <- getItem(tableName, pk(adamItem)).execute
              } yield assert(updated)(equalTo(Some(Item(name -> notAdam, id -> second, number -> 2)))) &&
                assert(updatedResponse)(equalTo(Some(Item(name -> adam))))
            }
          },
          testM("update name return all old") {
            withDefaultTable { tableName =>
              for {
                updatedResponse <- updateItem(tableName, pk(adamItem))($(name).setValue(notAdam))
                                     .returns(ReturnValues.AllOld)
                                     .execute
                updated         <- getItem(tableName, pk(adamItem)).execute
              } yield assert(updated)(equalTo(Some(Item(name -> notAdam, id -> second, number -> 2)))) &&
                assert(updatedResponse)(equalTo(Some(adamItem)))
            }
          },
          testM("update name return all new") {
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
          testM("update name return updated new") {
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
          testM("insert item into list") {
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
          testM("append to list") {
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
          testM("prepend to list") {
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
          testM("set an Item Attribute") {
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
            testM("field does not exist") {
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
            testM("does not update if field does exist") {
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
        testM("remove field") {
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
        testM("remove item from list") {
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
        testM("add item to set") {
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
        testM("add number using add action") {
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
        testM("add number using set action") {
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
        testM("subtract number") {
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
          testM("put item") {
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
          testM("condition check succeeds") {
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
          testM("condition check fails because 'id' != 'first'") {
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
                conditionCheck.zip(putItem).transaction.execute.run
              )(fails(assertDynamoDbException("ConditionalCheckFailed")))
            }
          },
          testM("delete item") {
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
          testM("delete item with where clause") {
            withDefaultTable { tableName =>
              val deleteItem = DeleteItem(
                key = pk(avi3Item),
                tableName = TableName(tableName)
              ).where($("firstName").beginsWith("avi"))
              for {
                _       <- deleteItem.transaction.execute
                written <- getItem(tableName, PrimaryKey(id -> first, number -> 7)).execute
              } yield assert(written)(isNone)
            }
          },
          testM("update item") {
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
          testM("all transaction types at once") {
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
          testM("two updates to same item fails") {
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

              assertM(updateItem1.zip(updateItem2).transaction.execute.run)(
                fails(assertDynamoDbException("Transaction request cannot include multiple operations on one item"))
              )
            }
          },
          testM("repeated client request token with different transaction fails") {
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

              assertM(program.run)(
                fails(isSubtype[IdempotentParameterMismatchException](Assertion.anything))
              )
            }
          }
        ),
        suite("transact get items")(
          testM("basic transact get items") {
            withDefaultTable { tableName =>
              val getItems =
                GetItem(TableName(tableName), pk(avi3Item))
                  .zip(GetItem(TableName(tableName), pk(adam2Item)))
              for {
                a <- getItems.transaction.execute
              } yield assert(a)(equalTo((Some(avi3Item), Some(adam2Item))))
            }
          },
          testM("basic batch get item transaction") {
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
          testM("missing item does not result in failure") {
            withDefaultTable { tableName =>
              val getItems =
                GetItem(TableName(tableName), Item(id -> first, number -> 1000))
                  .zip(GetItem(TableName(tableName), pk(adam2Item)))
              for {
                a <- getItems.transaction.execute
              } yield assert(a)(equalTo((None, Some(adam2Item))))
            }
          },
          testM("missing item in other table") {
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
        layer.orDie
      ) @@ nondeterministic
}
