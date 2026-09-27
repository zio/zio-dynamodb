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

package examples

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.dynamodb._

/**
 * Interpreter-level retry defaults — see the "Retries" reference doc. `defaultRetryPolicy`
 * governs every query on an interpreter that doesn't set its own `.withRetryPolicy(...)`.
 */
object RetryPolicyDefaults extends ZIOAppDefault {

  // A ZIO Schedule-native alternative to the shipped default, reusing zio.Schedule directly
  // instead of RetryPolicy's own constructors.
  val scheduleBacked: EffectfulRetryPolicy[Task] =
    ZioRetryPolicies.fromSchedule(Schedule.exponential(50.millis) && Schedule.recurs(5))

  // No .withRetryPolicy of its own — governed entirely by whichever interpreter runs it.
  val batchQuery: DynamoDBQuery[Any, Batch.GetResult] =
    DynamoDBQuery.batchGetItem(List("ord-1", "ord-2"))(id =>
      DynamoDBQuery.GetItem("orders", PrimaryKey("orderId" -> id))
    )

  def logBatchResult(label: String)(result: Batch.GetResult): Task[Unit] = result match {
    case Batch.GetResult.Complete(_)         => ZIO.logInfo(s"$label: batch completed")
    case Batch.GetResult.Incomplete(_)       => ZIO.logWarning(s"$label: unprocessed keys left as-is")
    case Batch.GetResult.Failed(cause, _, _) => ZIO.logError(s"$label: batch failed: $cause")
  }

  def run: Task[Unit] =
    ZIO.scoped {
      for {
        client <-
          ZIO.acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)

        // opts every query on this interpreter out of retrying, unless a query sets its own policy
        noRetryInterp = ZioInterpreter.fromAsyncClient(client, defaultRetryPolicy = None)
        _ <- noRetryInterp.run(DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")))
        // batch: any unprocessed keys AWS returns stay unprocessed — no response-level retry loop runs
        _ <- noRetryInterp.run(batchQuery).flatMap(logBatchResult("no-retry"))

        // every query on this interpreter retries via the Schedule-backed policy by default
        scheduleInterp = ZioInterpreter.fromAsyncClient(client, defaultRetryPolicy = Some(scheduleBacked))
        _ <- scheduleInterp.run(DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-2")))
        // batch: unprocessed keys are resubmitted through the Schedule-backed policy automatically
        _ <- scheduleInterp.run(batchQuery).flatMap(logBatchResult("schedule-backed"))
      } yield ()
    }
}
