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

package examples.highlevelapi

import zio.dynamodb.{ DummyIOInterpreter, Interpreter }
import zio.dynamodb.ExecuteSyntax._
import zio.test._

/** Runs each of `KeyConditions`'s six CRUD queries — no network call, no Docker. */
object KeyConditionsSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("KeyConditions — CRUD queries execute")(
    test("all six CRUD queries execute") {
      val ran = for {
        _ <- KeyConditions.putQuery.execute
        _ <- KeyConditions.getQuery.execute
        _ <- KeyConditions.updateQuery.execute
        _ <- KeyConditions.deleteQuery.execute
        _ <- KeyConditions.queryQuery.execute
        _ <- KeyConditions.scanQuery.execute
      } yield assertCompletes
      ran.unsafeRun()
    }
  )
}
