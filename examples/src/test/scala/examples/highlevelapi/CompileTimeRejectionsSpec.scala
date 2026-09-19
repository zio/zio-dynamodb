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
import zio.test.Assertion._

/**
 * `CompileTimeRejections.buildsFineFailsOnRun` builds without complaint (nothing type-gates a
 * `Map`'s key type), but running it exercises the actual failure the rest of that file only
 * describes in a comment: DynamoDB requires string map keys, so the query fails once it runs.
 */
object CompileTimeRejectionsSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("CompileTimeRejections — live failure")(
    test("a non-String map key builds fine but fails once the query runs") {
      val outcome = scala.util.Try(CompileTimeRejections.buildsFineFailsOnRun.execute.unsafeRun())
      assertTrue(outcome.isFailure) &&
      assert(outcome.failed.get.getMessage)(containsString("only String keys are supported in DDB"))
    }
  )
}
