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

import zio.dynamodb.DynamoDBError.QueryBuilderError.InvalidPath
import zio.dynamodb.ProjectionExpression.$
import zio.test._
import zio.test.Assertion._

import scala.util.Try

object InvalidPathSpec extends ZIOSpecDefault {

  private val table = "t"
  private val key   = PrimaryKey("id" -> "1")

  // Built at the top level, outside any effect: an invalid path must not throw here.
  private val bad    = $("a..b")
  private val badIdx = $("a[x]")
  private val good   = $("name")

  // DummyIO's fail throws the DynamoDBError, so a failed run surfaces as a Left.
  private def run(q: DynamoDBQuery[_, _]): Either[Throwable, Any] = Try(DummyIOInterpreter.run(q).unsafeRun()).toEither

  private def failsWithInvalidPath(q: DynamoDBQuery[_, _], fragment: String) =
    assert(run(q))(isLeft(isSubtype[InvalidPath](hasField("message", _.message, containsString(fragment)))))

  def spec = suite("InvalidPath")(
    suite("$")(
      test("an invalid path doesn't throw while building") {
        assert(Try($("a..b")).toEither)(isRight(anything))
      },
      test("the error names the path") {
        failsWithInvalidPath(DynamoDBQuery.getItem(table, key, bad), "a..b")
      },
      test("a valid path still runs") {
        assert(run(DynamoDBQuery.getItem(table, key, good)))(isRight(anything))
      },
      test("errors from several paths are reported together") {
        failsWithInvalidPath(DynamoDBQuery.getItem(table, key, bad, badIdx), "a..b") &&
        failsWithInvalidPath(DynamoDBQuery.getItem(table, key, bad, badIdx), "a[x]")
      },
      test("extending an invalid path keeps it invalid") {
        failsWithInvalidPath(DynamoDBQuery.getItem(table, key, bad("c")), "a..b")
      }
    ),
    suite("is caught wherever a path is used")(
      test("getItem projections") {
        failsWithInvalidPath(DynamoDBQuery.getItem(table, key, bad), "a..b")
      },
      test("putItem condition") {
        failsWithInvalidPath(DynamoDBQuery.putItem(table, key).where(bad.attributeExists), "a..b")
      },
      test("deleteItem condition, as a comparison operand") {
        failsWithInvalidPath(DynamoDBQuery.deleteItem(table, key).where(bad === 1), "a..b")
      },
      test("updateItem actions") {
        def update(action: UpdateExpression.Action[Any]) =
          DynamoDBQuery.updateItem(table, key, UpdateExpression(action))
        failsWithInvalidPath(update(bad.set(1)), "a..b") &&
        failsWithInvalidPath(update(good.set(bad)), "a..b") &&
        failsWithInvalidPath(update(good.setIfNotExists(bad, 1)), "a..b") &&
        failsWithInvalidPath(update(bad.appendList(List(1))), "a..b") &&
        failsWithInvalidPath(update(bad.remove), "a..b") &&
        failsWithInvalidPath(update(bad.add(1)), "a..b") &&
        failsWithInvalidPath(update(good.set(1) + bad.remove), "a..b")
      },
      test("updateItem condition") {
        failsWithInvalidPath(
          DynamoDBQuery.updateItem(table, key, UpdateExpression(good.set(1))).where(bad.beginsWith("x")),
          "a..b"
        )
      },
      test("query filter and projections") {
        failsWithInvalidPath(
          DynamoDBQuery.query(table, 10).whereKey($("id").partitionKey === "1").filter(bad > 1),
          "a..b"
        ) &&
        failsWithInvalidPath(
          DynamoDBQuery
            .Query(table, 10, keyConditionExpr = Some($("id").partitionKey === "1"), projections = List(bad)),
          "a..b"
        )
      },
      test("scan filter, including size, between, in and nested conditions") {
        def scan(ce: ConditionExpression[Any]) = DynamoDBQuery.scan(table, 10).filter(ce)
        failsWithInvalidPath(scan(bad.size > 1), "a..b") &&
        failsWithInvalidPath(scan(bad.between(1, 2)), "a..b") &&
        failsWithInvalidPath(scan(bad.in(1, 2)), "a..b") &&
        failsWithInvalidPath(scan(good.attributeExists && !(bad.contains("x"))), "a..b")
      },
      test("scan projections") {
        failsWithInvalidPath(DynamoDBQuery.scan(table, 10, projections = List(bad)), "a..b")
      },
      test("batchGetItem projections") {
        failsWithInvalidPath(
          DynamoDBQuery.batchGetItem(List("1"))(id => DynamoDBQuery.getItem(table, PrimaryKey("id" -> id), bad)),
          "a..b"
        )
      },
      test("transactGetItems projections") {
        failsWithInvalidPath(DynamoDBQuery.transactGetItems(DynamoDBQuery.GetItem(table, key, List(bad))), "a..b")
      },
      test("transactWriteItems conditions on a wrapped write") {
        failsWithInvalidPath(
          DynamoDBQuery.transactWriteItems(DynamoDBQuery.putItem(table, key).where(bad.attributeExists)),
          "a..b"
        )
      },
      test("inside map and zipPar") {
        val invalid = DynamoDBQuery.getItem(table, key, bad)
        failsWithInvalidPath(invalid.map(_.isDefined), "a..b") &&
        failsWithInvalidPath(DynamoDBQuery.getItem(table, key) zipPar invalid, "a..b")
      },
      test("transactWriteItems conditions, through a conditionCheck") {
        failsWithInvalidPath(
          DynamoDBQuery.transactWriteItems(DynamoDBQuery.conditionCheck(table, key)(bad.attributeExists)),
          "a..b"
        )
      }
    ),
    suite("keys")(
      test("a partition key from an invalid path") {
        failsWithInvalidPath(DynamoDBQuery.query(table, 10).whereKey(bad.partitionKey === "1"), "a..b")
      },
      test("a partition key must be a single top-level name") {
        failsWithInvalidPath(DynamoDBQuery.query(table, 10).whereKey($("a.b").partitionKey === "1"), "partition key") &&
        failsWithInvalidPath(DynamoDBQuery.query(table, 10).whereKey($("a[0]").partitionKey === "1"), "partition key")
      },
      test("a sort key must be a single top-level name, in each key condition") {
        val pk = $("id").partitionKey === "1"
        failsWithInvalidPath(DynamoDBQuery.query(table, 10).whereKey(pk && $("a.b").sortKey === 1), "sort key") &&
        failsWithInvalidPath(DynamoDBQuery.query(table, 10).whereKey(pk && $("a.b").sortKey > 1), "sort key") &&
        failsWithInvalidPath(DynamoDBQuery.query(table, 10).whereKey(pk && $("a.b").sortKey.between(1, 2)), "sort key")
      },
      test("valid keys still run") {
        assert(run(DynamoDBQuery.query(table, 10).whereKey($("id").partitionKey === "1" && $("ts").sortKey > 1)))(
          isRight(anything)
        )
      }
    )
  )
}
