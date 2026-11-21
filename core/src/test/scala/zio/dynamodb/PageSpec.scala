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

import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.test._

object PageSpec extends ZIOSpecDefault {

  def spec = suite("Page")(
    test("is covariant: a page of DecodingErrors is also a page of ItemErrors") {
      val failure                                   = DecodingError.failure("bad item")
      val decoded: Page[Either[DecodingError, Int]] = Page(Chunk(Right(1), Left(failure)), None, 2, 2)
      val widened: Page[Either[ItemError, Int]]     = decoded // compiles only because Page is covariant
      assertTrue(widened.items == Chunk(Right(1), Left(failure)))
    }
  )
}
