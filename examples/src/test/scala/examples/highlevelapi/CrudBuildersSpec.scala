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

/** Runs every query `CrudBuilders` builds — no network call, no Docker. */
object CrudBuildersSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("CrudBuilders — every builder executes")(
    test("every query executes") {
      val ran = for {
        _ <- CrudBuilders.putQuery.execute
        _ <- CrudBuilders.putWithCondition.execute
        _ <- CrudBuilders.getQuery.execute
        _ <- CrudBuilders.deleteQuery.execute
        _ <- CrudBuilders.deleteWithCondition.execute
        _ <- CrudBuilders.updateQuery.execute
        _ <- CrudBuilders.updateWithCondition.execute
        _ <- CrudBuilders.updateActionQuery.execute
        _ <- CrudBuilders.queryQuery.execute
        _ <- CrudBuilders.scanQuery.execute
      } yield assertCompletes
      ran.unsafeRun()
    }
  )
}
