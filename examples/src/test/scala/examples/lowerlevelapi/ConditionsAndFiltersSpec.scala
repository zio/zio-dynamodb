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

import zio.dynamodb.{ DummyIOInterpreter, DynamoDBError, Interpreter }
import zio.dynamodb.ExecuteSyntax._
import zio.test._

object ConditionsAndFiltersSpec extends ZIOSpecDefault {

  implicit val interpreter: Interpreter[zio.dynamodb.DummyIO] = DummyIOInterpreter

  def spec = suite("ConditionsAndFilters")(
    test("put.where, scan.filter, and delete.where execute") {
      val ran = for {
        _ <- ConditionsAndFilters.putWithCondition.execute
        _ <- ConditionsAndFilters.scanWithFilter.execute
        _ <- ConditionsAndFilters.deleteWithNegatedCondition.execute
      } yield assertCompletes
      ran.unsafeRun()
    },
    test("live failure: the naively-misused .filter on a putItem compiles but fails once run") {
      // Confirms the caveat documented on ConditionsAndFilters: an unpinned condition defeats
      // the compile-time CanFilter check, but every interpreter still rejects it at run time.
      val thrown = scala.util.Try(ConditionsAndFilters.naivelyMisusedFilter.execute.unsafeRun()).failed.toOption
      assertTrue(thrown.exists(_.isInstanceOf[DynamoDBError.QueryBuilderError.UnsupportedModifier]))
    }
  )
}
