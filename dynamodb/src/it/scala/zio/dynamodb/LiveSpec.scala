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
import zio.test.{ assert, DefaultRunnableSpec, TestResult, ZSpec }
import zio.test.Assertion._
import zio.test.environment._
import zio.duration._
import software.amazon.awssdk.regions.Region

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

  val adamPrimaryKey                                                         = PrimaryKey("id" -> "second", "age" -> 2)
  def insertPeople(tableName: String)                                        =
    putItem(tableName, Item("id" -> "first", "firstName" -> "avi", "age" -> 1)) *>
      putItem(tableName, Item("id" -> "first", "firstName" -> "anotherAvi", "age" -> 4)) *>
      putItem(tableName, Item("id" -> "second", "firstName" -> "adam", "age" -> 2)) *>
      putItem(tableName, Item("id" -> "third", "firstName" -> "john", "age" -> 3))

  def insertQueryData(tableName: String)                                     =
    putItem(tableName, Item("id" -> "first", "firstName" -> "avi", "age" -> 1)) *>
      putItem(tableName, Item("id" -> "first", "firstName" -> "anotherAvi", "age" -> 4)) *>
      putItem(tableName, Item("id" -> "first", "firstName" -> "yetAnotherAvi", "age" -> 7)) *>
      putItem(tableName, Item("id" -> "second", "firstName" -> "adam", "age" -> 2)) *>
      putItem(tableName, Item("id" -> "second", "firstName" -> "anotherAdam", "age" -> 5)) *>
      putItem(tableName, Item("id" -> "second", "firstName" -> "yetAnotherAdam", "age" -> 8)) *>
      putItem(tableName, Item("id" -> "third", "firstName" -> "john", "age" -> 3)) *>
      putItem(tableName, Item("id" -> "third", "firstName" -> "anotherJohn", "age" -> 6)) *>
      putItem(tableName, Item("id" -> "third", "firstName" -> "yetAnotherJohn", "age" -> 9))

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
    createTable(tableName, KeySchema("id", "age"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("id"),
      AttributeDefinition.attrDefnNumber("age")
    )

  def numberTable(tableName: String) =
    createTable(tableName, KeySchema("id"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnNumber("id")
    )

  private def managedTable(seed: Long, tableDefinition: String => CreateTable) =
    ZManaged
      .make(
        for {
          _         <- TestRandom.setSeed(seed)
          tableName <- random.nextUUID.map(_.toString)
          _         <- tableDefinition(tableName).execute
        } yield TableName(tableName)
      )(tName => deleteTable(tName.value).execute.orDie)

  def withTemporaryTable(
    tableDefinition: String => CreateTable,
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    // TODO(adam): This is bad random
    managedTable(scala.util.Random.nextLong(), tableDefinition).use(table => f(table.value))

  def withQueryPopulatedTable(
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    managedTable(scala.util.Random.nextLong(), defaultTable).use { table =>
      for {
        _      <- insertQueryData(table.value).execute
        result <- f(table.value)
      } yield result
    }

  def withDefaultPopulatedTable(
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    managedTable(scala.util.Random.nextLong(), defaultTable).use { table =>
      for {
        _      <- insertPeople(table.value).execute
        result <- f(table.value)
      } yield result
    }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("live test")(
      testM("put and get item") {
        withTemporaryTable(
          defaultTable,
          tableName =>
            for {
              _      <- putItem(tableName, Item("id" -> "first", "testName" -> "put and get item", "age" -> 20)).execute
              result <- getItem(tableName, PrimaryKey("id" -> "first", "age" -> 20)).execute
            } yield assert(result)(equalTo(Some(Item("id" -> "first", "testName" -> "put and get item", "age" -> 20))))
        )
      },
      testM("scan table") {
        withDefaultPopulatedTable { tableName =>
          for {
            stream <- scanAllItem(tableName).execute
            chunk  <- stream.runCollect
          } yield assert(chunk)(
            equalTo(
              Chunk(
                Item("id" -> "second", "firstName" -> "adam", "age"       -> 2),
                Item("id" -> "third", "firstName"  -> "john", "age"       -> 3),
                Item("id" -> "first", "firstName"  -> "avi", "age"        -> 1),
                Item("id" -> "first", "firstName"  -> "anotherAvi", "age" -> 4)
              )
            )
          )
        }
      },
      suite("query tables")(
        testM("query less than") {
          withDefaultPopulatedTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $("firstName"))
                              .whereKey(PartitionKey("id") === "first" && SortKey("age") < 2)
                              .execute
            } yield assert(chunk)(
              equalTo(Chunk(Item("firstName" -> "avi")))
            )
          }
        },
        testM("query table greater than") {
          withDefaultPopulatedTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $("firstName"))
                              .whereKey(PartitionKey("id") === "first" && SortKey("age") > 0)
                              .execute

            } yield assert(chunk)(
              equalTo(Chunk(Item("firstName" -> "avi"), Item("firstName" -> "anotherAvi")))
            ) // REVIEW(john): somehow getting chunk out of bound exception with empty queries (when results are empty)
          }
        },
        testM("SortKeyCondition between") {
          withQueryPopulatedTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $("firstName"))
                              .whereKey(PartitionKey("id") === "first" && SortKey("age").between(3, 8))
                              .execute
            } yield assert(chunk)(
              equalTo(Chunk(Item("firstName" -> "anotherAvi"), Item("firstName" -> "yetAnotherAvi")))
            )
          }
        }
      ),
      suite("update items")(
        testM("update name") {
          withDefaultPopulatedTable { tableName =>
            for {
              _       <- updateItem(tableName, adamPrimaryKey)($("firstName").set("notAdam")).execute
              updated <- getItem(
                           tableName,
                           adamPrimaryKey
                         ).execute // TODO(adam): for some reason adding a projection expression here results in none
            } yield assert(updated)(equalTo(Some(Item("id" -> "second", "age" -> 2, "firstName" -> "notAdam"))))
          }
        },
        testM("remove field from row") {
          withDefaultPopulatedTable { tableName =>
            for {
              _       <- updateItem(tableName, adamPrimaryKey)($("firstName").remove).execute
              updated <- getItem(
                           tableName,
                           adamPrimaryKey
                         ).execute
            } yield assert(updated)(equalTo(Some(Item("id" -> "second", "age" -> 2))))
          }
        },
        testM("insert item into list") {
          withDefaultPopulatedTable { tableName =>
            for {
              _       <- updateItem(tableName, adamPrimaryKey)($("listThing").set(List(1))).execute
              _       <- updateItem(tableName, adamPrimaryKey)($("listThing[1]").set(2)).execute
              updated <- getItem(
                           tableName,
                           adamPrimaryKey
                         ).execute
            } yield assert(updated)(
              equalTo(Some(Item("id" -> "second", "age" -> 2, "firstName" -> "adam", "listThing" -> List(1, 2))))
            )
          }
        },
        testM("append to list") {
          withDefaultPopulatedTable {
            tableName =>
              for {
                _       <- updateItem(tableName, adamPrimaryKey)($("listThing").set(List(1))).execute
                _       <- updateItem(tableName, adamPrimaryKey)($("listThing").appendList(Chunk(2, 3, 4))).execute
                // REVIEW(john): Getting None when a projection expression is added here
                updated <- getItem(tableName, adamPrimaryKey).execute
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
                _       <- updateItem(tableName, adamPrimaryKey)($("listThing").set(List(1))).execute
                _       <- updateItem(tableName, adamPrimaryKey)($("listThing").prependList(Chunk(-1, 0))).execute
                updated <- getItem(tableName, adamPrimaryKey).execute
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
                _       <- putItem(tableName, Item("id" -> 1, "num" -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey("id" -> 1))(
                             SetAction(
                               $("num"),
                               SetOperand.PathOperand($("num")) + SetOperand.ValueOperand(
                                 AttributeValue.Number(5)
                               )
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey("id" -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item("id" -> 1, "num" -> 5))))
          )
        },
        testM("subtract number") {
          withTemporaryTable(
            numberTable,
            tableName =>
              for {
                _       <- putItem(tableName, Item("id" -> 1, "num" -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey("id" -> 1))(
                             SetAction(
                               $("num"),
                               SetOperand.PathOperand($("num")) - SetOperand.ValueOperand(
                                 AttributeValue.Number(2)
                               )
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey("id" -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item("id" -> 1, "num" -> -2))))
          )
        },
        suite("if not exists")(
          testM("field does not exist") {
            withTemporaryTable(
              numberTable,
              tableName =>
                for {
                  _       <- putItem(tableName, Item("id" -> 1)).execute
                  _       <- updateItem(tableName, PrimaryKey("id" -> 1))($("num").setIfNotExists($("num"), 4)).execute
                  updated <- getItem(tableName, PrimaryKey("id" -> 1)).execute
                } yield assert(updated)(equalTo(Some(Item("id" -> 1, "num" -> 4))))
            )
          },
          testM("field does exist") {
            withTemporaryTable(
              numberTable,
              tableName =>
                for {
                  _       <- putItem(tableName, Item("id" -> 1, "num" -> 0)).execute
                  _       <- updateItem(tableName, PrimaryKey("id" -> 1))($("num").setIfNotExists($("num"), 4)).execute
                  updated <- getItem(tableName, PrimaryKey("id" -> 1)).execute
                } yield assert(updated)(equalTo(Some(Item("id" -> 1, "num" -> 0))))
            )
          }
        )
      )
    )
      .provideCustomLayerShared(layer.orDie)
//      .provideCustomLayerShared(liveAws.orDie)
}
