package zio.dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import io.github.vigoo.zioaws.core.config
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import zio.blocking.Blocking
import zio.clock.Clock
import zio.dynamodb.UpdateExpression.Action.SetAction
import zio.dynamodb.UpdateExpression.SetOperand
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.SortKeyExpression.SortKey
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression._
import zio.test.Assertion._
import zio.test.environment._
import zio.duration._
import software.amazon.awssdk.regions.Region
import zio.test._
import zio.test.TestAspect._

import java.net.URI

object LiveSpec extends DefaultRunnableSpec {

  val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  val liveAws = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live

  val layer: ZLayer[Any, Throwable, Has[DynamoDBProxyServer] with Has[DynamoDBExecutor]] =
    ((http4s.default ++ awsConfig) >>> config.configured() >>> (dynamodb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    } >>> DynamoDBExecutor.live)) ++ (Blocking.live >>> LocalDdbServer.inMemoryLayer)

  private val id       = "id"
  private val first    = "first"
  private val second   = "second"
  private val third    = "third"
  private val name     = "firstName"
  private val number   = "num"
  val secondPrimaryKey = PrimaryKey(id -> second, number -> 2)

  private val avi            = "avi"
  private val anotherAvi     = "avi2"
  private val yetAnotherAvi  = "avi3"
  private val adam           = "adam"
  private val anotherAdam    = "adam2"
  private val yetAnotherAdam = "adam3"
  private val john           = "john"
  private val anotherJohn    = "john2"
  private val yetAnotherJohn = "john3"

  def insertPeople(tableName: String)                                        =
    putItem(tableName, Item(id -> first, name -> avi, number -> 1)) *>
      putItem(tableName, Item(id -> first, name -> anotherAvi, number -> 4)) *>
      putItem(tableName, Item(id -> second, name -> adam, number -> 2)) *>
      putItem(tableName, Item(id -> third, name -> john, number -> 3))

  def insertQueryData(tableName: String)                                     =
    putItem(tableName, Item(id -> first, name -> avi, number -> 1)) *>
      putItem(tableName, Item(id -> first, name -> anotherAvi, number -> 4)) *>
      putItem(tableName, Item(id -> first, name -> yetAnotherAvi, number -> 7)) *>
      putItem(tableName, Item(id -> second, name -> adam, number -> 2)) *>
      putItem(tableName, Item(id -> second, name -> anotherAdam, number -> 5)) *>
      putItem(tableName, Item(id -> second, name -> yetAnotherAdam, number -> 8)) *>
      putItem(tableName, Item(id -> third, name -> john, number -> 3)) *>
      putItem(tableName, Item(id -> third, name -> anotherJohn, number -> 6)) *>
      putItem(tableName, Item(id -> third, name -> yetAnotherJohn, number -> 9))

  def awaitTableCreation(
    tableName: String
  ): ZIO[Has[DynamoDBExecutor] with Clock, Throwable, DescribeTableResponse] =
    describeTable(tableName).execute.flatMap { res =>
      res.tableStatus match {
        case TableStatus.Active => ZIO.succeed(res)
        case _                  => ZIO.fail(new Throwable("table not ready"))
      }
    }
      .retry(Schedule.spaced(2.seconds) && Schedule.recurs(5))

  private def defaultTable(tableName: String) =
    createTable(tableName, KeySchema(id, number), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString(id),
      AttributeDefinition.attrDefnNumber(number)
    )

  def numberTable(tableName: String) =
    createTable(tableName, KeySchema(id), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnNumber(id)
    )

  private def managedTable(tableDefinition: String => CreateTable) =
    ZManaged
      .make(
        for {
          tableName <- random.nextUUID.map(_.toString)
          _         <- tableDefinition(tableName).execute
        } yield TableName(tableName)
      )(tName => deleteTable(tName.value).execute.orDie)

  def withTemporaryTable(
    tableDefinition: String => CreateTable,
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    managedTable(tableDefinition).use(table => f(table.value))

  def withQueryPopulatedTable(
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    managedTable(defaultTable).use { table =>
      for {
        _      <- insertQueryData(table.value).execute
        result <- f(table.value)
      } yield result
    }

  def withDefaultPopulatedTable(
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    managedTable(defaultTable).use { table =>
      for {
        _      <- insertPeople(table.value).execute
        result <- f(table.value)
      } yield result
    }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("live test")(
      suite("basic us" + number)(
        testM("put and get item") {
          withTemporaryTable(
            defaultTable,
            tableName =>
              for {
                _      <- putItem(tableName, Item(id -> first, "testName" -> "put and get item", number -> 20)).execute
                result <- getItem(tableName, PrimaryKey(id -> first, number -> 20)).execute
              } yield assert(result)(
                equalTo(Some(Item(id -> first, "testName" -> "put and get item", number -> 20)))
              )
          )
        },
        testM("get nonexistant returns empty") {
          withDefaultPopulatedTable { tableName =>
            getItem(tableName, PrimaryKey(id -> "nowhere", number -> 1000)).execute.map(item => assert(item)(isNone))
          }
        }
      ),
      suite("scan tables")(
        testM("scan table") {
          withDefaultPopulatedTable { tableName =>
            for {
              stream <- scanAllItem(tableName).execute
              chunk  <- stream.runCollect
            } yield assert(chunk)(
              equalTo(
                Chunk(
                  Item(id -> second, name -> adam, number       -> 2),
                  Item(id -> third, name  -> john, number       -> 3),
                  Item(id -> first, name  -> avi, number        -> 1),
                  Item(id -> first, name  -> anotherAvi, number -> 4)
                )
              )
            )
          }
        }
      ),
      suite("query tables")(
        testM("query less than") {
          withDefaultPopulatedTable { tableName =>
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
          withDefaultPopulatedTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === first && SortKey(number) > 0)
                              .execute

            } yield assert(chunk)(
              equalTo(Chunk(Item(name -> avi), Item(name -> anotherAvi)))
            )
          }
        },
        testM("empty query result returns empty chunk") {
          withDefaultPopulatedTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === "nowhere" && SortKey(number) > 0)
                              .execute
            } yield assert(chunk)(isEmpty)
          }
        } @@ ignore, // Suspect that there is an edge case in the auto batch/auto parallelize code that could be causing this issue
        testM("query with limit") {
          withQueryPopulatedTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 1, $(name))
                              .whereKey(PartitionKey(id) === first)
                              .execute
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi))))
          }
        } @@ ignore, // limit is not being honored
        testM("query starting from StartKey") {
          withQueryPopulatedTable { tableName =>
            for {
              (_, startKey) <- querySomeItem(tableName, 2, $(id), $(number))
                                 .whereKey(PartitionKey(id) === first)
                                 .execute
              (chunk, _)    <- querySomeItem(tableName, 5, $(name))
                                 .whereKey(PartitionKey(id) === first)
                                 .startKey(startKey)
                                 .execute
            } yield assert(chunk)(equalTo(Chunk(Item(name -> yetAnotherAvi))))
          }
        } @@ ignore, // does not look like limit is being honored
        suite("SortKeyCondition")(
          testM("SortKeyCondition between") {
            withQueryPopulatedTable { tableName =>
              for {
                (chunk, _) <- querySomeItem(tableName, 10, $(name))
                                .whereKey(PartitionKey(id) === first && SortKey(number).between(3, 8))
                                .execute
              } yield assert(chunk)(
                equalTo(Chunk(Item(name -> anotherAvi), Item(name -> yetAnotherAvi)))
              )
            }
          }
        )
      ),
      suite("update items")(
        // add an @@ ignore annotation
        testM("update name") {
          withDefaultPopulatedTable {
            tableName =>
              for {
                _       <- updateItem(tableName, secondPrimaryKey)($(name).set("notAdam")).execute
                updated <- getItem(
                             tableName,
                             secondPrimaryKey
                           ).execute // TODO(adam): for some reason adding a projection expression here results in none
                // Expected to be a bug somewhere -- possibly write a ticket
              } yield assert(updated)(equalTo(Some(Item(name -> "notAdam", id -> second, number -> 2))))
          }
        },
        testM("remove field") {
          withDefaultPopulatedTable { tableName =>
            for {
              _       <- updateItem(tableName, secondPrimaryKey)($(name).remove).execute
              updated <- getItem(
                           tableName,
                           secondPrimaryKey
                         ).execute
            } yield assert(updated)(equalTo(Some(Item(id -> second, number -> 2))))
          }
        },
        testM("insert item into list") {
          withDefaultPopulatedTable { tableName =>
            for {
              _       <- updateItem(tableName, secondPrimaryKey)($("listThing").set(List(1))).execute
              _       <- updateItem(tableName, secondPrimaryKey)($("listThing[1]").set(2)).execute
              updated <- getItem(
                           tableName,
                           secondPrimaryKey
                         ).execute
            } yield assert(updated)(
              equalTo(Some(Item(id -> second, number -> 2, name -> adam, "listThing" -> List(1, 2))))
            )
          }
        },
        testM("append to list") {
          withDefaultPopulatedTable {
            tableName =>
              for {
                _       <- updateItem(tableName, secondPrimaryKey)($("listThing").set(List(1))).execute
                _       <- updateItem(tableName, secondPrimaryKey)($("listThing").appendList(Chunk(2, 3, 4))).execute
                // REVIEW(john): Getting None when a projection expression is added here
                updated <- getItem(tableName, secondPrimaryKey).execute
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
          withDefaultPopulatedTable {
            tableName =>
              for {
                _       <- updateItem(tableName, secondPrimaryKey)($("listThing").set(List(1))).execute
                _       <- updateItem(tableName, secondPrimaryKey)($("listThing").prependList(Chunk(-1, 0))).execute
                updated <- getItem(tableName, secondPrimaryKey).execute
              } yield assert(
                updated.map(a =>
                  a.get("listThing")(
                    FromAttributeValue.iterableFromAttributeValue(FromAttributeValue.intFromAttributeValue)
                  )
                )
              )(equalTo(Some(Right(List(-1, 0, 1)))))
          }
        },
        testM("add number") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "num" -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))(
                             SetAction(
                               $("num"),
                               SetOperand.PathOperand($("num")) + SetOperand.ValueOperand(
                                 AttributeValue.Number(5)
                               )
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "num" -> 5))))
          )
        },
        testM("subtract number") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _       <- putItem(tableName, Item(id -> 1, "num" -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))(
                             SetAction(
                               $("num"),
                               SetOperand.PathOperand($("num")) - SetOperand.ValueOperand(
                                 AttributeValue.Number(2)
                               )
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, "num" -> -2))))
          )
        },
        suite("if not exists")(
          testM("field does not exist") {
            withTemporaryTable(
              numberTable,
              tableName =>
                for {
                  _       <- putItem(tableName, Item(id -> 1)).execute
                  _       <- updateItem(tableName, PrimaryKey(id -> 1))($("num").setIfNotExists($("num"), 4)).execute
                  updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
                } yield assert(updated)(equalTo(Some(Item(id -> 1, "num" -> 4))))
            )
          },
          testM("does not update if field does exist") {
            withTemporaryTable(
              numberTable,
              tableName =>
                for {
                  _       <- putItem(tableName, Item(id -> 1, "num" -> 0)).execute
                  _       <- updateItem(tableName, PrimaryKey(id -> 1))($("num").setIfNotExists($("num"), 4)).execute
                  updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
                } yield assert(updated)(equalTo(Some(Item(id -> 1, "num" -> 0))))
            )
          }
        )
      )
    )
      .provideCustomLayerShared(
        layer.orDie
      ) @@ nondeterministic
  // @@ around(???)(???) // may not be able to use this, I need the results of the first effect in the test
//      .provideCustomLayerShared(liveAws.orDie)
}
