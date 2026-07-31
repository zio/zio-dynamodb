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

  // -- effect primitives (sleep/attempt/raiseError) — used internally by the
  // retry machinery; not exercised by any query since this module has no
  // dedicated retry spec (unlike zio's RetrySpec.scala).
  test("sleep completes after the given duration without blocking") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    interp.sleep(scala.concurrent.duration.Duration.Zero)
  }

  test("attempt wraps a successful IO in Right") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    interp.attempt(IO.pure(42)).map(r => assertEquals(r, Right(42)))
  }

  test("attempt wraps a failed IO in Left") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    val boom   = new RuntimeException("boom")
    interp.attempt(IO.raiseError(boom)).map(r => assertEquals(r, Left(boom)))
  }

  test("raiseError produces a failed IO with the given throwable") {
    val client = clientFixture()
    val interp = CEInterpreter.fromAsyncClient(client)
    val boom   = new RuntimeException("boom")
    interp.raiseError[Int](boom).attempt.map(r => assertEquals(r, Left(boom)))
  }

  // -- CEResponseInterceptor.accumulating -------------------------------------

  private def getItemMeta(table: String) =
    DynamoDBResponseMetadata.GetItem(tableName = table, consumed = None, correlation = CorrelationContext(None))

  private def putItemMeta(table: String) =
    DynamoDBResponseMetadata.PutItem(
      tableName = table,
      consumed = None,
      collectionMetrics = None,
      correlation = CorrelationContext(None)
    )

  test("CEResponseInterceptor.accumulating collects metadata entries in call order") {
    for {
      acc   <- CEResponseInterceptor.accumulating
      _     <- acc.interceptor.onResponse(getItemMeta("t"))
      _     <- acc.interceptor.onResponse(putItemMeta("t"))
      chunk <- acc.results
    } yield {
      assertEquals(chunk.length, 2)
      assert(chunk(0).isInstanceOf[DynamoDBResponseMetadata.GetItem])
      assert(chunk(1).isInstanceOf[DynamoDBResponseMetadata.PutItem])
    }
  }

  test("CEResponseInterceptor.accumulating: results is non-destructive") {
    for {
      acc    <- CEResponseInterceptor.accumulating
      _      <- acc.interceptor.onResponse(getItemMeta("t"))
      first  <- acc.results
      second <- acc.results
    } yield {
      assertEquals(first.length, 1)
      assertEquals(second.length, 1)
    }
  }

  test("CEResponseInterceptor.accumulating: fresh accumulator per call starts empty") {
    for {
      acc   <- CEResponseInterceptor.accumulating
      chunk <- acc.results
    } yield assert(chunk.isEmpty)
  }
}
