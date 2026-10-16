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

import org.openjdk.jmh.annotations._
import zio.dynamodb.ProjectionExpression.$

import java.util.concurrent.TimeUnit

/** Construction-only cost of Low-Level writes, as passed to an interpreter (`DynamoDBQuery`). */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class LowLevelWriteBench {
  private val table = "t"
  private val item  = Item("id" -> "a", "name" -> "b")
  private val key   = PrimaryKey("id" -> "a")

  @Benchmark def putConstructOnly: DynamoDBQuery[_, _] =
    DynamoDBQuery.putItem(table, item)

  @Benchmark def conditionalPutConstructOnly: DynamoDBQuery[_, _] =
    DynamoDBQuery.putItem(table, item).where($("name") === "b")

  @Benchmark def updateConstructOnly: DynamoDBQuery[_, _] =
    DynamoDBQuery.updateItem(table, key)($("name").set("c"))
}
