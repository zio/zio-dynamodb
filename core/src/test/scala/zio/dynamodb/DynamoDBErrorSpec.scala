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

import zio.dynamodb.DynamoDBError.ItemError._
import zio.test._

object DynamoDBErrorSpec extends ZIOSpecDefault {

  def spec = suite("DynamoDBError")(
    suite("ItemError")(
      test("ValueNotFound has message") {
        val e = ValueNotFound("key not found")
        assertTrue(e.message == "key not found")
      },

      test("DecodingError single-message factory has message") {
        val e = DecodingError("bad type")
        assertTrue(e.message == "bad type")
      },

      test("DecodingError.MissingField formats message") {
        val s = DecodingError.MissingField("name")
        assertTrue(s.message == "missing field 'name'")
      },

      test("DecodingError.TypeMismatch formats message") {
        val s = DecodingError.TypeMismatch("age", "Number", "String")
        assertTrue(s.message == "field 'age': expected Number, got String")
      },

      test("DecodingError composed from causes joins messages with semicolons") {
        val e = DecodingError(
          Array[DecodingError.Cause](
            DecodingError.MissingField("id"),
            DecodingError.TypeMismatch("age", "Number", "String")
          )
        )
        assertTrue(e.message == "missing field 'id'; field 'age': expected Number, got String")
      },

      test("DecodingError.++ merges two errors") {
        val a = DecodingError.missingField("id")
        val b = DecodingError.typeMismatch("age", "Number", "String")
        val c = a ++ b
        assertTrue(c.causes.length == 2)
      }
    )
  )
}
