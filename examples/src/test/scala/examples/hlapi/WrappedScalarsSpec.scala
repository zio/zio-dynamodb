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

package examples.hlapi

import zio.dynamodb.{ DummyIOInterpreter, Interpreter }
import zio.dynamodb.ExecuteSyntax._
import zio.test._

/** Runs each of `WrappedScalars`'s six CRUD queries — no network call, no Docker. */
object WrappedScalarsSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("WrappedScalars — CRUD queries execute")(
    test("all six CRUD queries execute") {
      val ran = for {
        _ <- WrappedScalars.putQuery.execute
        _ <- WrappedScalars.getQuery.execute
        _ <- WrappedScalars.updateQuery.execute
        _ <- WrappedScalars.deleteQuery.execute
        _ <- WrappedScalars.queryQuery.execute
        _ <- WrappedScalars.scanQuery.execute
      } yield assertCompletes
      ran.unsafeRun()
    }
  )
}
