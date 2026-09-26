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

import scala.concurrent.duration.FiniteDuration

/**
 * Request-level retry policies — see the "Retries" reference doc. Every interpreter already
 * retries on transient errors by default; `.withRetryPolicy` overrides that for one query.
 */
object RetryPolicyBasics extends ZIOAppDefault {

  // Stateless: delay grows linearly with the attempt number, five attempts.
  val linear: RetryPolicy =
    RetryPolicy.custom(attempt =>
      if (attempt >= 5) None else Some(FiniteDuration(200L * (attempt + 1), "milliseconds"))
    )

  // Stateful, pure: AWS's own decorrelated-jitter formula, tuned down from the shipped default.
  val shortAwsRecommended: RetryPolicy =
    RetryPolicy.awsRecommended(
      maxRetries = 3,
      baseDelay = FiniteDuration(50, "milliseconds"),
      maxDelay = FiniteDuration(2, "seconds")
    )

  def run: Task[Unit] =
    ZIO.scoped {
      for {
        client <-
          ZIO.acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        interp = ZioInterpreter.fromAsyncClient(client) // AWS-recommended default already attached
        _ <-
          interp.run(DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")).withRetryPolicy(linear))
        _ <-
          interp.run(
            DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-2")).withRetryPolicy(shortAwsRecommended)
          )
        _ <-
          interp.run(
            DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-3")).withRetryPolicy(RetryPolicy.NoRetry)
          )
      } yield ()
    }
}
