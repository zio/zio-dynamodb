/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import cats.effect.{ IO, Resource }
import munit.CatsEffectSuite
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import scala.concurrent.duration._
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition => AwsAttributeDefinition,
  BillingMode => AwsBillingMode,
  _
}

import java.net.URI
import java.util.UUID

class CEDynamoDBSpec extends CatsEffectSuite {

  override val munitIOTimeout: Duration = 3.minutes

  // --- Infrastructure resources ---

  private val containerResource: Resource[IO, DynamoDbAsyncClient] =
    Resource
      .make(
        IO {
          val c = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest")) {}
          c.addExposedPort(8000)
          c.setCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
          c.waitingFor(Wait.forListeningPort())
          c.start()
          c
        }
      )(c => IO(c.stop()))
      .map { c =>
        val endpoint = URI.create(s"http://${c.getHost}:${c.getMappedPort(8000)}")
        val creds    = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
        DynamoDbAsyncClient
          .builder()
          .endpointOverride(endpoint)
          .credentialsProvider(creds)
          .region(Region.US_EAST_1)
          .build()
      }

  // Shared across all tests in the suite — container starts once, stops after the last test.
  val clientFixture =
    ResourceSuiteLocalFixture("dynamodb-client", containerResource)

  override def munitFixtures = List(clientFixture)

  // Per-test table: created on acquire, deleted on release regardless of test outcome.
  private def tableResource(client: DynamoDbAsyncClient): Resource[IO, String] = {
    val tableName = s"test-${UUID.randomUUID()}"
    Resource.make(
      IO.fromCompletableFuture(
        IO(
          client.createTable(
            CreateTableRequest
              .builder()
              .tableName(tableName)
              .keySchema(
                KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build()
              )
              .attributeDefinitions(
                AwsAttributeDefinition
                  .builder()
                  .attributeName("id")
                  .attributeType(ScalarAttributeType.S)
                  .build()
              )
              .billingMode(AwsBillingMode.PAY_PER_REQUEST)
              .build()
          )
        )
      ).as(tableName)
    )(name =>
      IO.fromCompletableFuture(
        IO(client.deleteTable(DeleteTableRequest.builder().tableName(name).build()))
      ).void
        .handleError(_ => ())
    )
  }

  // --- Tests ---

  test("getItem returns None for a missing key") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    tableResource(client).use { table =>
      interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "missing"))).map { result =>
        assert(result.isEmpty)
      }
    }
  }

  test("putItem then getItem roundtrip") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    tableResource(client).use { table =>
      for {
        _      <- interp.run(DynamoDBQuery.putItem(table, Item("id" -> "alice", "score" -> 42)))
        result <- interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "alice")))
      } yield {
        assertEquals(result.flatMap(_.get[String]("id").toOption), Some("alice"))
        assertEquals(result.flatMap(_.get[Int]("score").toOption), Some(42))
      }
    }
  }

  test("putItem overwrites an existing item") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    tableResource(client).use { table =>
      for {
        _      <- interp.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "v" -> "first")))
        _      <- interp.run(DynamoDBQuery.putItem(table, Item("id" -> "bob", "v" -> "second")))
        result <- interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "bob")))
      } yield assertEquals(result.flatMap(_.get[String]("v").toOption), Some("second"))
    }
  }

  test("deleteItem removes an item") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    tableResource(client).use { table =>
      for {
        _      <- interp.run(DynamoDBQuery.putItem(table, Item("id" -> "charlie")))
        _      <- interp.run(DynamoDBQuery.deleteItem(table, PrimaryKey("id" -> "charlie")))
        result <- interp.run(DynamoDBQuery.getItem(table, PrimaryKey("id" -> "charlie")))
      } yield assert(result.isEmpty)
    }
  }

  test("scanSome returns items up to the limit") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    tableResource(client).use { table =>
      for {
        _    <- interp.run(DynamoDBQuery.putItem(table, Item("id" -> "a")))
        _    <- interp.run(DynamoDBQuery.putItem(table, Item("id" -> "b")))
        page <- interp.run(DynamoDBQuery.scanSome(table, limit = 10))
      } yield assertEquals(page.items.length, 2)
    }
  }

  test("createTable/describeTable/deleteTable lifecycle") {
    val client    = clientFixture()
    val interp    = CEInterpreter.fromAsyncClient(client)
    val tableName = s"lifecycle-${UUID.randomUUID()}"
    val attrs     = NonEmptySet(AttributeDefinition.attrDefnString("id"))
    for {
      _    <- interp.run(DynamoDBQuery.createTable(tableName, KeySchema("id"), attrs, BillingMode.PayPerRequest))
      desc <- interp.run(DynamoDBQuery.describeTable(tableName))
      _    <- interp.run(DynamoDBQuery.deleteTable(tableName))
    } yield assertEquals(desc.tableStatus, DynamoDBQuery.TableStatus.Active)
  }

  test("zipped getItem calls execute independently") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    tableResource(client).use { table =>
      val q = DynamoDBQuery.getItem(table, PrimaryKey("id" -> "x")) zipPar
        DynamoDBQuery.getItem(table, PrimaryKey("id" -> "y"))
      for {
        _    <- interp.run(DynamoDBQuery.putItem(table, Item("id" -> "x")))
        pair <- interp.run(q)
        (rx, ry) = pair
      } yield {
        assert(rx.isDefined)
        assert(ry.isEmpty)
      }
    }
  }
}
