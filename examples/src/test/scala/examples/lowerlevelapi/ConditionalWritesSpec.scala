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

package examples.lowerlevelapi

import zio.dynamodb.{ DummyIOInterpreter, Interpreter }
import zio.dynamodb.ExecuteSyntax._
import zio.test._

/**
 * Runs every query `ConditionalWrites` builds - no network call, no Docker. The stub never fails a
 * condition, so the per-effect `openAccount` / `transfer` handlers are checked by compiling, not run.
 */
object ConditionalWritesSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("ConditionalWrites — every query executes")(
    test("every query executes") {
      val ran = for {
        _ <- ConditionalWrites.putIfAbsent.execute
        _ <- ConditionalWrites.updateIfOpen.execute
        _ <- ConditionalWrites.deleteIfClosed.execute
        _ <- ConditionalWrites.updateWithoutItemOnFailure.execute
        _ <- ConditionalWrites.putReturningPrevious.execute
        _ <- ConditionalWrites.updateReturningNew.execute
        _ <- ConditionalWrites.deleteReturningDeleted.execute
        _ <- ConditionalWrites.withRequestOptions.execute
        _ <- ConditionalWrites.twoWritesInParallel.execute
        _ <- ConditionalWrites.guardedTransfer.execute
        _ <- ConditionalWrites.guardedTransferWithoutItems.execute
      } yield assertCompletes
      ran.unsafeRun()
    }
  )
}
