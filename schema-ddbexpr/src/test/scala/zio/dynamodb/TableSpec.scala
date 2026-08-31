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

import zio.blocks.schema.{ NameMapper, Schema }
import zio.dynamodb.blocks.ddbexpr.DdbExprApi
import zio.test._
import zio.test.Assertion._

object TableSpec extends ZIOSpecDefault {

  private final case class Person(firstName: String, lastName: String, age: Int)
  private object Person {
    implicit val schema: Schema[Person] = Schema.derived
  }

  private val ada = Person("Ada", "Lovelace", 36)

  def spec = suite("Table")(
    suite("decode / encode")(
      test("encode then decode round-trips through the configured codec") {
        val people = DdbExprApi.Table[Person]("people")
        assert(people.encode(ada).flatMap(people.decode))(isRight(equalTo(ada)))
      },
      test("decode surfaces a DecodingError for a malformed item rather than throwing") {
        val people = DdbExprApi.Table[Person]("people")
        val broken = Item("firstName" -> "Ada") // missing lastName / age
        assert(people.decode(broken))(isLeft(isSubtype[DynamoDBError.ItemError.DecodingError](anything)))
      }
    ),
    suite(".deriving affects decode / encode")(
      test("withFieldNameMapper changes the encoded attribute names, and decode reads them back") {
        val snake = DdbExprApi.Table[Person]("people").deriving(_.withFieldNameMapper(NameMapper.SnakeCase))
        val item  = snake.encode(ada)
        assert(item.map(_.map.keySet))(isRight(equalTo(Set("first_name", "last_name", "age")))) &&
        assert(item.flatMap(snake.decode))(isRight(equalTo(ada)))
      },
      test("two Table values for the same type carry independent config") {
        val plain = DdbExprApi.Table[Person]("people")
        val snake = DdbExprApi.Table[Person]("people").deriving(_.withFieldNameMapper(NameMapper.SnakeCase))
        assertTrue(plain.encode(ada).toOption.map(_.map.keySet) != snake.encode(ada).toOption.map(_.map.keySet))
      }
    )
  )
}
