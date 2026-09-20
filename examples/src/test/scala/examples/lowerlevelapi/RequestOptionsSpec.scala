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

object RequestOptionsSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("RequestOptions — configured queries execute")(
    test("all five configured queries execute") {
      val ran = for {
        _ <- RequestOptions.putWithCapacity.execute
        _ <- RequestOptions.updateWithReturnValues.execute
        _ <- RequestOptions.strictlyConsistentGet.execute
        _ <- RequestOptions.descendingQuery.execute
        _ <- RequestOptions.nextPage.execute
      } yield assertCompletes
      ran.unsafeRun()
    }
  )
}
