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

import scala.concurrent.duration.FiniteDuration
import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError

// `map`/`flatMap` exist so a query-composition test can sequence several `.execute` calls in
// a for-comprehension, same shape as it would under a real effect type - not a general-purpose
// effect type of its own.
final case class DummyIO[+A](unsafeRun: () => A) {
  def map[B](f: A => B): DummyIO[B]              = DummyIO(() => f(unsafeRun()))
  def flatMap[B](f: A => DummyIO[B]): DummyIO[B] = DummyIO(() => f(unsafeRun()).unsafeRun())
}
object DummyIO                                   {
  def succeed[A](a: => A): DummyIO[A] = DummyIO(() => a)
}

// -- DummyIO interpreter -------------------------------------------------
// Stub interpreter extending AwsInterpreter directly with hardcoded no-ops.
// No AWS SDK types involved; used for testing query-composition logic, and by BatchBench.
// class DummyIOInterpreter(client) — extends RealAwsInterpreter, lives in aws/test.

object DummyIOInterpreter extends AwsInterpreter[DummyIO] {

  private[dynamodb] def pure[A](a: A): DummyIO[A]                                     = DummyIO.succeed(a)
  private[dynamodb] def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              = fa.map(f)
  private[dynamodb] def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] = fa.flatMap(f)
  protected def product[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]        =
    DummyIO(() => (fa.unsafeRun(), fb.unsafeRun()))
  protected def productPar[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]     =
    DummyIO(() => (fa.unsafeRun(), fb.unsafeRun()))
  protected def fail[A](e: DynamoDBError): DummyIO[A]                                 =
    DummyIO(() => throw e)
  protected def absolve[A](fa: DummyIO[Either[ItemError, A]]): DummyIO[A]             =
    DummyIO(() =>
      fa.unsafeRun() match {
        case Right(a) => a
        case Left(e)  => throw e
      }
    )

  // sleep is a no-op in tests; attempt/raiseError wrap synchronous throws
  private[dynamodb] def sleep(d: FiniteDuration): DummyIO[Unit]                   =
    DummyIO.succeed(())
  private[dynamodb] def attempt[A](fa: DummyIO[A]): DummyIO[Either[Throwable, A]] =
    DummyIO(() => scala.util.Try(fa.unsafeRun()).toEither)
  private[dynamodb] def raiseError[A](t: Throwable): DummyIO[A]                   =
    DummyIO(() => throw t)

  protected def runGetItem(q: DynamoDBQuery.GetItem): DummyIO[Option[Item]]                                        = DummyIO.succeed(None)
  protected def runPutItem(q: DynamoDBQuery.PutItem): DummyIO[Option[Item]]                                        = DummyIO.succeed(None)
  protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): DummyIO[Option[Item]]                                  = DummyIO.succeed(None)
  protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): DummyIO[Option[Item]]                                  = DummyIO.succeed(None)
  protected def runQuery(q: DynamoDBQuery.Query): DummyIO[Page[Item]]                                              =
    DummyIO.succeed(Page(Chunk.empty, None, 0, 0))
  protected def runScan(q: DynamoDBQuery.Scan): DummyIO[Page[Item]]                                                =
    DummyIO.succeed(Page(Chunk.empty, None, 0, 0))
  protected def runCreateTable(q: DynamoDBQuery.CreateTable): DummyIO[Unit]                                        =
    DummyIO.succeed(())
  protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): DummyIO[Unit]                                        =
    DummyIO.succeed(())
  protected def runDescribeTable(q: DynamoDBQuery.DescribeTable): DummyIO[DynamoDBQuery.DescribeTableResponse]     =
    DummyIO.succeed(DynamoDBQuery.DescribeTableResponse("arn:dummy", DynamoDBQuery.TableStatus.Active, 0L, 0L))
  protected def runBatchGetItem(q: DynamoDBQuery.BatchGetItem): DummyIO[DynamoDBQuery.BatchGetItem.Response]       =
    DummyIO.succeed(DynamoDBQuery.BatchGetItem.Response())
  protected def runBatchWriteItem(q: DynamoDBQuery.BatchWriteItem): DummyIO[DynamoDBQuery.BatchWriteItem.Response] =
    DummyIO.succeed(DynamoDBQuery.BatchWriteItem.Response(None))
  protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): DummyIO[Chunk[Option[Item]]]               =
    DummyIO.succeed(Chunk.fill(q.getItems.length)(None))
  protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): DummyIO[Unit]                          =
    DummyIO.succeed(())
}
