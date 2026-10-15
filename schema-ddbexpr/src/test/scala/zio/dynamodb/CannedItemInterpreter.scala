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

import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError

import scala.concurrent.duration.FiniteDuration

// Answers every get/put/update/delete with `item` and records the request nodes it received, so
// HL decoding and the requests the builders produce can be checked without an AWS client.
final class CannedItemInterpreter(item: Option[Item]) extends AwsInterpreter[DummyIO] {
  private var received: List[DynamoDBQuery[_, _]] = Nil

  def requests: List[DynamoDBQuery[_, _]] = received.reverse

  private def answer(q: DynamoDBQuery[_, _]): DummyIO[Option[Item]] =
    DummyIO { () => received = q :: received; item }

  private[dynamodb] def pure[A](a: A): DummyIO[A]                                     = DummyIO.succeed(a)
  private[dynamodb] def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              = fa.map(f)
  private[dynamodb] def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] = fa.flatMap(f)
  protected def product[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]        =
    DummyIO(() => (fa.unsafeRun(), fb.unsafeRun()))
  protected def productPar[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]     = product(fa, fb)
  protected def fail[A](e: DynamoDBError): DummyIO[A]                                 = DummyIO(() => throw e)
  protected def absolve[A](fa: DummyIO[Either[ItemError, A]]): DummyIO[A]             =
    fa.map(_.fold(e => throw e, identity))
  private[dynamodb] def sleep(d: FiniteDuration): DummyIO[Unit]                       = DummyIO.succeed(())
  private[dynamodb] def attempt[A](fa: DummyIO[A]): DummyIO[Either[Throwable, A]]     =
    DummyIO(() => scala.util.Try(fa.unsafeRun()).toEither)
  private[dynamodb] def raiseError[A](t: Throwable): DummyIO[A]                       = DummyIO(() => throw t)

  protected def runGetItem(q: DynamoDBQuery.GetItem): DummyIO[Option[Item]]       = answer(q)
  protected def runPutItem(q: DynamoDBQuery.PutItem): DummyIO[Option[Item]]       = answer(q)
  protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): DummyIO[Option[Item]] = answer(q)
  protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): DummyIO[Option[Item]] = answer(q)

  private def unused[A](q: DynamoDBQuery[_, _]): DummyIO[A] =
    DummyIO(() => throw new UnsupportedOperationException(s"not stubbed: $q"))

  protected def runQuery(q: DynamoDBQuery.Query): DummyIO[Page[Item]]                                              = unused(q)
  protected def runScan(q: DynamoDBQuery.Scan): DummyIO[Page[Item]]                                                = unused(q)
  protected def runCreateTable(q: DynamoDBQuery.CreateTable): DummyIO[Unit]                                        = unused(q)
  protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): DummyIO[Unit]                                        = unused(q)
  protected def runDescribeTable(q: DynamoDBQuery.DescribeTable): DummyIO[DynamoDBQuery.DescribeTableResponse]     = unused(
    q
  )
  protected def runBatchGetItem(q: DynamoDBQuery.BatchGetItem): DummyIO[DynamoDBQuery.BatchGetItem.Response]       = unused(q)
  protected def runBatchWriteItem(q: DynamoDBQuery.BatchWriteItem): DummyIO[DynamoDBQuery.BatchWriteItem.Response] =
    unused(q)
  protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): DummyIO[Chunk[Option[Item]]]               = unused(q)
  protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): DummyIO[Unit]                          = unused(q)
}
