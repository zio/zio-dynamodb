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

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.time.Duration
import zio._
import zio.test.{ TestResult, ZIOSpec }

import java.net.URI
import java.util.UUID

// Bundles the async client (for admin ops) with a Task-based interpreter (for query ops).
final case class DynamoDBEnv(asyncClient: DynamoDbAsyncClient, interpreter: Interpreter[Task])

// Starts a DynamoDB Local container once per suite and exposes it as a DynamoDbAsyncClient.
// Concrete specs extend this, wire an env layer with their chosen interpreter, and call
// withSingleIdKeyTable / withIdAndYearKeyTable in each test.
abstract class DynamoDBLocalSpec extends ZIOSpec[DynamoDbAsyncClient] {

  override val bootstrap: ZLayer[Any, Throwable, DynamoDbAsyncClient] =
    ZLayer.scoped {
      ZIO
        .acquireRelease(
          ZIO.attempt {
            val container = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest")) {}
            container.addExposedPort(8000)
            container.setCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
            container.waitingFor(Wait.forListeningPort())
            container.start()
            container
          }
        )(c => ZIO.succeed(c.stop()))
        .map { container =>
          val endpoint = URI.create(s"http://${container.getHost}:${container.getMappedPort(8000)}")
          val creds    = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
          // connectionMaxIdleTime(1ms) prevents Netty from reusing connections between
          // sequential test requests, avoiding the LastHttpContent encoder state bug.
          DynamoDbAsyncClient
            .builder()
            .endpointOverride(endpoint)
            .credentialsProvider(creds)
            .region(Region.US_EAST_1)
            .httpClientBuilder(
              NettyNioAsyncHttpClient
                .builder()
                .connectionMaxIdleTime(Duration.ofMillis(1))
            )
            .build()
        }
    }

  // -- Table structure definitions -----------------------------------------

  protected def singleIdKeyTable(tableName: String): DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.createTable(
      tableName,
      KeySchema("id"),
      NonEmptySet(AttributeDefinition.attrDefnString("id")),
      BillingMode.PayPerRequest
    )

  protected def idAndYearKeyTable(tableName: String): DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.createTable(
      tableName,
      KeySchema("id", "year"),
      NonEmptySet(AttributeDefinition.attrDefnString("id"), AttributeDefinition.attrDefnString("year")),
      BillingMode.PayPerRequest
    )

  protected def idTableWithCategoryGsi(tableName: String): DynamoDBQuery[Any, Unit] =
    DynamoDBQuery
      .createTable(
        tableName,
        KeySchema("id"),
        NonEmptySet(AttributeDefinition.attrDefnString("id"), AttributeDefinition.attrDefnString("category")),
        BillingMode.PayPerRequest
      )
      .gsi("category-index", KeySchema("category"), ProjectionType.All)

  protected def idYearTableWithScoreLsi(tableName: String): DynamoDBQuery[Any, Unit] =
    DynamoDBQuery
      .createTable(
        tableName,
        KeySchema("id", "year"),
        NonEmptySet(
          AttributeDefinition.attrDefnString("id"),
          AttributeDefinition.attrDefnString("year"),
          AttributeDefinition.attrDefnString("score")
        ),
        BillingMode.PayPerRequest
      )
      .lsi("score-index", KeySchema("id", "score"))

  // -- Scoped table lifecycle -----------------------------------------------
  // Creates a UUID-named table on acquire; deletes it on scope exit regardless of outcome.

  protected def managedTable(
    tableDefinition: String => DynamoDBQuery[Any, Unit]
  ): ZIO[DynamoDBEnv & Scope, Throwable, String] =
    ZIO.acquireRelease(
      for {
        env <- ZIO.service[DynamoDBEnv]
        tableName = s"test-${UUID.randomUUID()}"
        _   <- env.interpreter.run(tableDefinition(tableName))
      } yield tableName
    )(tableName => ZIO.serviceWithZIO[DynamoDBEnv](_.interpreter.run(DynamoDBQuery.deleteTable(tableName))).orDie)

  // -- Convenience wrappers ------------------------------------------------

  protected def withSingleIdKeyTable(
    f: (String, Interpreter[Task]) => ZIO[Any, Throwable, TestResult]
  ): ZIO[DynamoDBEnv, Throwable, TestResult] =
    ZIO.scoped {
      for {
        table  <- managedTable(singleIdKeyTable)
        env    <- ZIO.service[DynamoDBEnv]
        result <- f(table, env.interpreter)
      } yield result
    }

  protected def withIdAndYearKeyTable(
    f: (String, Interpreter[Task]) => ZIO[Any, Throwable, TestResult]
  ): ZIO[DynamoDBEnv, Throwable, TestResult] =
    ZIO.scoped {
      for {
        table  <- managedTable(idAndYearKeyTable)
        env    <- ZIO.service[DynamoDBEnv]
        result <- f(table, env.interpreter)
      } yield result
    }

  protected def withGsiTable(
    f: (String, Interpreter[Task]) => ZIO[Any, Throwable, TestResult]
  ): ZIO[DynamoDBEnv, Throwable, TestResult] =
    ZIO.scoped {
      for {
        table  <- managedTable(idTableWithCategoryGsi)
        env    <- ZIO.service[DynamoDBEnv]
        result <- f(table, env.interpreter)
      } yield result
    }

  protected def withLsiTable(
    f: (String, Interpreter[Task]) => ZIO[Any, Throwable, TestResult]
  ): ZIO[DynamoDBEnv, Throwable, TestResult] =
    ZIO.scoped {
      for {
        table  <- managedTable(idYearTableWithScoreLsi)
        env    <- ZIO.service[DynamoDBEnv]
        result <- f(table, env.interpreter)
      } yield result
    }
}
