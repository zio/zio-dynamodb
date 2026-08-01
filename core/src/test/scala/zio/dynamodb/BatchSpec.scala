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

import zio.test._

/**
 * runWriteItem/runGetItem are typed to only accept a BatchWriteItem/
 *  BatchGetItem query, so their "wrong query type" defensive branch is
 *  unreachable through normal, type-safe construction — force it here via
 *  asInstanceOf, mirroring how a caller could still hit it by holding a
 *  DynamoDBQuery[Any, _] value of the wrong runtime shape.
 */
object BatchSpec extends ZIOSpecDefault {

  def spec = suite("Batch")(
    test("runWriteItem fails with IllegalArgumentException when given a non-BatchWriteItem query") {
      val notABatch = DynamoDBQuery
        .getItem("t", PrimaryKey("id" -> "a"))
        .asInstanceOf[DynamoDBQuery[Any, DynamoDBQuery.BatchWriteItem.Response]]
      Batch.runWriteItem(DummyIOInterpreter)(notABatch).unsafeRun() match {
        case Batch.WriteResult.Failed(cause, responseRetries, effectRetries) =>
          assertTrue(
            cause.isInstanceOf[IllegalArgumentException],
            cause.getMessage.contains("Expected BatchWriteItem"),
            responseRetries == 0,
            effectRetries == 0
          )
        case other                                                           =>
          assertNever(s"expected WriteResult.Failed, got $other")
      }
    },
    test("runGetItem fails with IllegalArgumentException when given a non-BatchGetItem query") {
      val notABatch = DynamoDBQuery
        .putItem("t", Item("id" -> "a"))
        .asInstanceOf[DynamoDBQuery[Any, DynamoDBQuery.BatchGetItem.Response]]
      Batch.runGetItem(DummyIOInterpreter)(notABatch).unsafeRun() match {
        case Batch.GetResult.Failed(cause, responseRetries, effectRetries) =>
          assertTrue(
            cause.isInstanceOf[IllegalArgumentException],
            cause.getMessage.contains("Expected BatchGetItem"),
            responseRetries == 0,
            effectRetries == 0
          )
        case other                                                         =>
          assertNever(s"expected GetResult.Failed, got $other")
      }
    }
  )
}
