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

import zio.test._

/**
 * Shapes the Low-Level API rejects at compile time, verified for real via `zio.test.typeCheck`:
 * a value with no `ToAttributeValue` instance, and a `CanWhere`/`CanFilter` violation once the
 * condition's own type parameter is pinned to a concrete type — see `ConditionsAndFilters.scala`
 * for the companion case where leaving it unpinned defeats the check instead.
 */
object CompileTimeRejectionsSpec extends ZIOSpecDefault {

  def spec = suite("Low-Level API compile-time rejections")(
    test("=== against a value with no ToAttributeValue instance is rejected") {
      typeCheck("""
        import zio.dynamodb.ProjectionExpression._
        final case class NoInstance(x: Int)
        $("field") === NoInstance(1)
      """).map(result => assertTrue(result.isLeft))
    },
    test(".filter on a putItem compiles when the condition is left unpinned (type Any)") {
      // The caveat from ConditionsAndFilters: an unascribed `$(...)`-built condition infers as
      // ConditionExpression[Any], and Any trivially satisfies CanFilter for any operation.
      typeCheck("""
        import zio.dynamodb.{ DynamoDBQuery, Item }
        import zio.dynamodb.ProjectionExpression._
        DynamoDBQuery.putItem("t", Item("id" -> "1")).filter($("id") === "1")
      """).map(result => assertTrue(result.isRight))
    },
    test(".filter on a putItem is rejected once the condition is pinned to a concrete type") {
      typeCheck("""
        import zio.dynamodb.{ ConditionExpression, DynamoDBQuery, Item }
        import zio.dynamodb.ProjectionExpression._
        final case class Widget(id: String)
        val pinned: ConditionExpression[Widget] = $("id") === "1"
        DynamoDBQuery.putItem("t", Item("id" -> "1")).filter(pinned)
      """).map(result => assertTrue(result.isLeft))
    },
    test(".where on a scan is rejected once the condition is pinned to a concrete type") {
      typeCheck("""
        import zio.dynamodb.{ ConditionExpression, DynamoDBQuery }
        import zio.dynamodb.ProjectionExpression._
        final case class Widget(id: String)
        val pinned: ConditionExpression[Widget] = $("id") === "1"
        DynamoDBQuery.scan("t", limit = 10).where(pinned)
      """).map(result => assertTrue(result.isLeft))
    }
  )
}
