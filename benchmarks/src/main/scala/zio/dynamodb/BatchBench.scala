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

import java.util.concurrent.TimeUnit

/**
 * Low-Level batch queries at AWS's per-call limits (25 writes, 100 gets): construction only, and a
 * run through `DummyIOInterpreter`, whose stubbed responses leave only the library's own overhead.
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class BatchBench {
  private val table = "t"
  private val items = (1 to 25).map(i => Item("id" -> i.toString, "name" -> "b")).toList
  private val ids   = (1 to 100).map(_.toString).toList

  private val prebuiltWrite = DynamoDBQuery.batchWriteItem(items)(DynamoDBQuery.putItem(table, _))
  private val prebuiltGet   = DynamoDBQuery.batchGetItem(ids)(id => DynamoDBQuery.getItem(table, PrimaryKey("id" -> id)))

  @Benchmark def batchWrite25ConstructOnly: DynamoDBQuery[_, _] =
    DynamoDBQuery.batchWriteItem(items)(DynamoDBQuery.putItem(table, _))

  @Benchmark def batchGet100ConstructOnly: DynamoDBQuery[_, _] =
    DynamoDBQuery.batchGetItem(ids)(id => DynamoDBQuery.getItem(table, PrimaryKey("id" -> id)))

  @Benchmark def batchGet100StrongConstructOnly: DynamoDBQuery[_, _] =
    DynamoDBQuery.batchGetItem(ids)(id =>
      DynamoDBQuery.getItem(table, PrimaryKey("id" -> id)).consistency(ConsistencyMode.Strong)
    )

  @Benchmark def batchWrite25RunPrebuilt: Batch.WriteResult =
    DummyIOInterpreter.run(prebuiltWrite).unsafeRun()

  @Benchmark def batchGet100RunPrebuilt: Batch.GetResult =
    DummyIOInterpreter.run(prebuiltGet).unsafeRun()
}
