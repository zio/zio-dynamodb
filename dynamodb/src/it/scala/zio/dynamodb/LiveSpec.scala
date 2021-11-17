package zio.dynamodb

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import io.github.vigoo.zioaws.core.config
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import zio.blocking.Blocking
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

  private val avi   = "avi"
  private val avi2  = "avi2"
  private val avi3  = "avi3"
  private val adam  = "adam"
  private val adam2 = "adam2"
  private val adam3 = "adam3"
  private val john  = "john"
  private val john2 = "john2"
  private val john3 = "john3"

  private val aviItem  = Item(id -> first, name -> avi, number -> 1)
  private val avi2Item = Item(id -> first, name -> avi2, number -> 4)
  private val avi3Item = Item(id -> first, name -> avi3, number -> 7)

  private val adamItem  = Item(id -> second, name -> adam, number -> 2)
  private val adam2Item = Item(id -> second, name -> adam2, number -> 5)
  private val adam3Item = Item(id -> second, name -> adam3, number -> 8)

  private val johnItem  = Item(id -> third, name -> john, number -> 3)
  private val john2Item = Item(id -> third, name -> john2, number -> 6)
  private val john3Item = Item(id -> third, name -> john3, number -> 9)

  def insertData(tableName: String) =
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

  def withDefaultTable(
    f: String => ZIO[Has[DynamoDBExecutor], Throwable, TestResult]
  ) =
    managedTable(defaultTable).use { table =>
      for {
        _      <- insertData(table.value).execute
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
          withDefaultTable { tableName =>
            getItem(tableName, PrimaryKey(id -> "nowhere", number -> 1000)).execute.map(item => assert(item)(isNone))
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
                Chunk(
                  adamItem,
                  adam2Item,
                  adam3Item,
                  johnItem,
                  john2Item,
                  john3Item,
                  aviItem,
                  avi2Item,
                  avi3Item
                )
              )
            )
          }
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
        testM("empty query result returns empty chunk") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 10, $(name))
                              .whereKey(PartitionKey(id) === "nowhere" && SortKey(number) > 0)
                              .execute
            } yield assert(chunk)(isEmpty)
          }
        } @@ ignore, // Suspect that there is an edge case in the auto batch/auto parallelize code that could be causing this issue
        testM("query with limit") {
          withDefaultTable { tableName =>
            for {
              (chunk, _) <- querySomeItem(tableName, 1, $(name))
                              .whereKey(PartitionKey(id) === first)
                              .execute
            } yield assert(chunk)(equalTo(Chunk(Item(name -> avi))))
          }
        } @@ ignore, // limit is not being honored
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
        } @@ ignore, // does not look like limit is being honored
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
          }
        )
      ),
      suite("update items")(
        // add an @@ ignore annotation
        testM("update name") {
          withDefaultTable {
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
          withDefaultTable { tableName =>
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
          withDefaultTable { tableName =>
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
          withDefaultTable {
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
          withDefaultTable {
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
                _       <- putItem(tableName, Item(id -> 1, number -> 0)).execute
                _       <- updateItem(tableName, PrimaryKey(id -> 1))(
                             SetAction(
                               $("num"),
                               SetOperand.PathOperand($("num")) + SetOperand.ValueOperand(
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
                               $("num"),
                               SetOperand.PathOperand($("num")) - SetOperand.ValueOperand(
                                 AttributeValue.Number(2)
                               )
                             )
                           ).execute
                updated <- getItem(tableName, PrimaryKey(id -> 1)).execute
              } yield assert(updated)(equalTo(Some(Item(id -> 1, number -> -2))))
          )
        },
        suite("if not exists")(
          testM("field does not exist") {
            withTemporaryTable(
              numberTable,
              tableName =>
                for {
                  _       <- putItem(tableName, Item(id -> 1)).execute
                  _       <- updateItem(tableName, PrimaryKey(id -> 1))($(number).setIfNotExists($(number), 4)).execute
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
      )
    )
      .provideCustomLayerShared(
        layer.orDie
      ) @@ nondeterministic
  // @@ around(???)(???) // may not be able to use this, I need the results of the first effect in the test
//      .provideCustomLayerShared(liveAws.orDie)
}
