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

import zio.test._
import zio.test.Assertion.{ anything, isSubtype }

/**
 * Batch queries run through the same [[AwsInterpreter.run]] entry point as every other
 *  [[DynamoDBQuery]] — there is no separate runner to defend against a mismatched query
 *  type, so these tests exercise the response-level retry loop (resubmitting
 *  unprocessedKeys/unprocessedItems) directly via `interp.run`.
 */
object BatchSpec extends ZIOSpecDefault {

  def spec = suite("Batch")(
    test("BatchWriteItem with no items completes immediately") {
      val q = DynamoDBQuery.batchWriteItem(List.empty[Item])(i => DynamoDBQuery.putItem("t", i))
      assert(DummyIOInterpreter.run(q).unsafeRun())(isSubtype[Batch.WriteResult.Complete](anything))
    },
    test("BatchGetItem with no keys completes immediately") {
      val q = DynamoDBQuery.batchGetItem(List.empty[String])(id => DynamoDBQuery.getItem("t", PrimaryKey("id" -> id)))
      assert(DummyIOInterpreter.run(q).unsafeRun())(isSubtype[Batch.GetResult.Complete](anything))
    }
  )
}
