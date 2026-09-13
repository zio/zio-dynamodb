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

package zio.dynamodb.external

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.dynamodb._
import zio.test._

// Deliberately outside `zio.dynamodb` — every other spec extending DynamoDBLocalSpec shares
// that package and could always see `private[dynamodb]` members regardless of this bug.
// `createTable` previously took a `NonEmptySet[AttributeDefinition]`, a type whose constructor
// and companion are both `private[dynamodb]`, so no caller outside the package could ever
// construct a valid argument for it — this spec is the regression coverage for that being
// fixed (a public `head, tail*` signature, mirroring `ProjectionType.Include`, keeps
// NonEmptySet itself internal while still requiring at least one AttributeDefinition).
object CreateTablePublicApiSpec extends DynamoDBLocalSpec {

  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  def spec = suite("createTable is callable from outside zio.dynamodb")(
    test("creates and describes a table using the public head + tail* signature") {
      for {
        env       <- ZIO.service[DynamoDBEnv]
        tableName = s"external-${java.util.UUID.randomUUID()}"
        _         <- env.interpreter.run(
                       DynamoDBQuery.createTable(tableName, KeySchema("id"), AttributeDefinition.attrDefnString("id"))(
                         BillingMode.PayPerRequest
                       )
                     )
        described <- env.interpreter.run(DynamoDBQuery.describeTable(tableName))
        _         <- env.interpreter.run(DynamoDBQuery.deleteTable(tableName))
      } yield assertTrue(described.tableArn.endsWith(tableName))
    }
  ).provideSome[DynamoDbAsyncClient](envLayer)
}
