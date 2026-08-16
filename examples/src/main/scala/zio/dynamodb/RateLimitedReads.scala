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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.blocks.chunk.Chunk
import zio.stream.ZStream

/**
 * zio-dynamodb concentrates on core DynamoDB concerns — building and running requests,
 * reporting accurate capacity data back via [[ResponseInterceptor]] — and leaves non-core
 * concerns like rate limiting to application code. `rcuRateLimiter` below is exactly that:
 * plain user code, not a library API, meant to be copied and adapted rather than imported.
 *
 * It's a [[Ref]]-backed token bucket keyed on wall-clock time ([[Clock.nanoTime]], so it's
 * `TestClock`-adjustable), weighted by *RCUs actually consumed* per response rather than
 * raw call count — deliberately not adapted from a generic call-rate limiter, since a call
 * that costs 1 RCU and one that costs 40 RCU shouldn't be throttled identically. Only read
 * capacity counts; write operations don't draw down the bucket.
 *
 * Because [[ResponseInterceptor.onResponse]]'s effect is sequenced before the caller
 * receives its result (see [[InterceptingAwsDynamoDB]]), a sleep here genuinely delays
 * when the next operation in a sequential pipeline can be issued — which is exactly what
 * makes it effective paired with [[ZIOStreamingUtils.batchGetItems]] below: each batch's
 * `onResponse` delay blocks the stream from pulling the next batch. It does *not* gate a
 * burst of concurrent/parallel calls (`zipPar`, `foreachParDiscard`): each one's AWS
 * request has already gone out by the time its own `onResponse` runs, so a shared bucket
 * only throttles when results become visible, not when requests get dispatched.
 *
 * The bucket starts full (`rcusPerSecond` tokens) and never accrues more than one
 * second's worth of burst credit, so a caller that's been idle doesn't get to spend a
 * large backlog of banked capacity in one burst after resuming.
 */
object RateLimitedReads extends ZIOAppDefault {

  def rcuRateLimiter(rcusPerSecond: Double): UIO[ResponseInterceptor[Task]] =
    for {
      now <- Clock.nanoTime
      ref <- Ref.make((rcusPerSecond, now))
    } yield new ResponseInterceptor[Task] {
      def onResponse(meta: DynamoDBResponseMetadata): Task[Unit] =
        for {
          now        <- Clock.nanoTime
          consumed = readCapacityUnitsOf(meta)
          sleepNanos <- ref.modify { case (tokens, lastNanos) =>
                          val elapsedSec   = math.max(0.0, (now - lastNanos) / 1e9)
                          val refilled     = math.min(tokens + elapsedSec * rcusPerSecond, rcusPerSecond)
                          val remaining    = refilled - consumed
                          // How much of a concurrently-racing response's reservation is still
                          // pending beyond `now` — never let a new reservation move the shared
                          // deadline backward and silently cancel part of it.
                          val pendingNanos = math.max(0L, lastNanos - now)
                          if (remaining < 0.0) {
                            val waitNanos = pendingNanos + ((-remaining / rcusPerSecond) * 1e9).toLong
                            (waitNanos, (0.0, now + waitNanos))
                          } else
                            (pendingNanos, (remaining, math.max(now, lastNanos)))
                        }
          _          <- ZIO.sleep(Duration.fromNanos(sleepNanos)).when(sleepNanos > 0L)
        } yield ()
    }

  private def readCapacityUnitsOf(meta: DynamoDBResponseMetadata): Double = {
    def rcu(consumed: Option[ConsumedCapacity]): Double     = consumed.flatMap(_.readCapacityUnits).getOrElse(0.0)
    def rcuBatch(consumed: Chunk[ConsumedCapacity]): Double =
      consumed.foldLeft(0.0)((acc, c) => acc + c.readCapacityUnits.getOrElse(0.0))
    meta match {
      case m: DynamoDBResponseMetadata.GetItem            => rcu(m.consumed)
      case m: DynamoDBResponseMetadata.PutItem            => rcu(m.consumed)
      case m: DynamoDBResponseMetadata.UpdateItem         => rcu(m.consumed)
      case m: DynamoDBResponseMetadata.DeleteItem         => rcu(m.consumed)
      case m: DynamoDBResponseMetadata.Query              => rcu(m.consumed)
      case m: DynamoDBResponseMetadata.Scan               => rcu(m.consumed)
      case m: DynamoDBResponseMetadata.BatchGetItem       => rcuBatch(m.consumed)
      case m: DynamoDBResponseMetadata.BatchWriteItem     => rcuBatch(m.consumed)
      case m: DynamoDBResponseMetadata.TransactGetItems   => rcuBatch(m.consumed)
      case m: DynamoDBResponseMetadata.TransactWriteItems => rcuBatch(m.consumed)
    }
  }

  def run: Task[Unit] =
    for {
      interceptor <- rcuRateLimiter(rcusPerSecond = 5.0)
      interp = ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build(), interceptor)
      keys = ZStream.fromIterable((1 to 200).map(i => PrimaryKey("orderId" -> s"ord-$i")))
      // batchGetItems groups keys into batches of 100 and issues one BatchGetItem per
      // group via interp.run — since interp carries the rate limiter, each batch's
      // onResponse delay gates when the stream can pull the next group.
      _           <- ZIOStreamingUtils.batchGetItems(interp, "orders")(keys).runDrain
    } yield ()
}
