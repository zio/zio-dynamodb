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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import zio._
import zio.test.Assertion._
import zio.test._

import scala.concurrent.ExecutionContext

object FutureInterpreterFailureSpec extends DynamoDBLocalSpec {

  def spec = suite("FutureInterpreter failures")(
    test("fail the Future with the SDK exception itself, not a CompletionException around it") {
      for {
        client <- ZIO.service[DynamoDbAsyncClient]
        interp = FutureInterpreter.fromAsyncClient(client)(ExecutionContext.global)
        result <-
          ZIO.fromFuture(_ => interp.run(DynamoDBQuery.getItem("no-such-table", PrimaryKey("id" -> "a")))).either
      } yield assert(result)(isLeft(isSubtype[ResourceNotFoundException](anything)))
    }
  )
}
