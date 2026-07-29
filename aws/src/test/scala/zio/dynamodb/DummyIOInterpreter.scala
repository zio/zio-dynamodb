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

import zio.dynamodb.DynamoDBError.ItemError

import scala.concurrent.duration.FiniteDuration

// Wires a synthetic AwsDynamoDB[DummyIO] through RealAwsInterpreter so that
// codec and error-handling paths are exercised in unit tests without a real SDK client.
final class DummyIOInterpreter(client: AwsDynamoDB[DummyIO]) extends RealAwsInterpreter[DummyIO](client) {
  private[dynamodb] def pure[A](a: A): DummyIO[A]                                     = DummyIO.succeed(a)
  private[dynamodb] def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              =
    DummyIO(() => f(fa.unsafeRun()))
  private[dynamodb] def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] =
    DummyIO(() => f(fa.unsafeRun()).unsafeRun())
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

  private[dynamodb] def sleep(d: FiniteDuration): DummyIO[Unit]                   =
    DummyIO.succeed(())
  private[dynamodb] def attempt[A](fa: DummyIO[A]): DummyIO[Either[Throwable, A]] =
    DummyIO(() => scala.util.Try(fa.unsafeRun()).toEither)
  private[dynamodb] def raiseError[A](t: Throwable): DummyIO[A]                   =
    DummyIO(() => throw t)
}
