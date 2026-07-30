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

import zio._
import zio.stream.ZStream

object StreamingUtils {

  private val BatchSize = 100

  /**
   * Reads items from DynamoDB for a stream of primary keys using batched reads.
   *
   *  Keys are grouped into batches of up to 100 and submitted via [[Batch.runGetItem]].
   *  The stream never fails: incomplete and failed batches are logged (ZIO logger) and
   *  skipped, and pagination continues with the next batch.
   *
   *  @param interp     the effect interpreter to run each batch through
   *  @param tableName  DynamoDB table to query
   *  @param policy     retry policy passed to [[Batch.runGetItem]]; defaults to [[RetryPolicy.NoRetry]]
   *  @param keys       source stream of primary keys to fetch
   */
  def batchGetItems(
    interp: AwsInterpreter[Task],
    tableName: String,
    policy: RetryPolicy = RetryPolicy.NoRetry
  )(keys: ZStream[Any, Nothing, PrimaryKey]): ZStream[Any, Nothing, Item] =
    keys
      .grouped(BatchSize)
      .mapZIO { chunk =>
        val query = DynamoDBQuery
          .batchGetItem(chunk)(pk => DynamoDBQuery.GetItem(tableName, pk))
          .withRetryPolicy(policy)
        Batch.runGetItem(interp)(query).flatMap {
          case Batch.GetResult.Complete(response) =>
            ZIO.succeed(response.responses.iterator.flatMap(_._2).toList)

          case Batch.GetResult.Incomplete(response) =>
            val unprocessedCount = response.unprocessedKeys.values.map(_.keysSet.size).sum
            ZIO.logWarning(
              s"batchGetItems[$tableName]: $unprocessedCount key(s) unprocessed after policy exhaustion"
            ) *> ZIO.succeed(response.responses.iterator.flatMap(_._2).toList)

          case Batch.GetResult.Failed(cause, responseRetries, effectRetries) =>
            ZIO.logError(
              s"batchGetItems[$tableName]: batch failed " +
                s"(responseRetries=$responseRetries, effectRetries=$effectRetries): ${cause.getMessage}"
            ) *> ZIO.succeed(Nil)
        }
      }
      .orDie
      .flatMap(ZStream.fromIterable(_))
}
