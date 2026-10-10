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

import software.amazon.awssdk.services.dynamodb.model.{
  ReturnValuesOnConditionCheckFailure => AwsReturnValuesOnConditionCheckFailure
}
import zio.dynamodb.ProjectionExpression.$
import zio.test._

object AwsCodecsReturnValuesOnConditionCheckFailureSpec extends ZIOSpecDefault {

  private val pk    = PrimaryKey("id" -> "a")
  private val item  = Item("id" -> "a")
  private val table = "t"

  private val allOld: ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.AllOld

  def spec = suite("AwsCodecs returnValuesOnConditionCheckFailure wiring")(
    suite("toPutItemRequest")(
      test("AllOld → ALL_OLD") {
        val q   = DynamoDBQuery.putItem(table, item).where($("v") === 1).returnValuesOnConditionCheckFailure(allOld)
        val req = AwsCodecs.toPutItemRequest(q.asInstanceOf[DynamoDBQuery.PutItem])
        assertTrue(req.returnValuesOnConditionCheckFailure() == AwsReturnValuesOnConditionCheckFailure.ALL_OLD)
      },
      test("unset → not sent") {
        val req = AwsCodecs.toPutItemRequest(DynamoDBQuery.PutItem(table, item))
        assertTrue(req.returnValuesOnConditionCheckFailure() == null)
      }
    ),
    suite("toUpdateItemRequest")(
      test("AllOld → ALL_OLD") {
        val q   = DynamoDBQuery
          .updateItem(table, pk)($("v").set(2))
          .where($("v") === 1)
          .returnValuesOnConditionCheckFailure(allOld)
        val req = AwsCodecs.toUpdateItemRequest(q.asInstanceOf[DynamoDBQuery.UpdateItem])
        assertTrue(req.returnValuesOnConditionCheckFailure() == AwsReturnValuesOnConditionCheckFailure.ALL_OLD)
      },
      test("unset → not sent") {
        val req = AwsCodecs.toUpdateItemRequest(DynamoDBQuery.UpdateItem(table, pk, UpdateExpression($("v").set(2))))
        assertTrue(req.returnValuesOnConditionCheckFailure() == null)
      }
    ),
    suite("toDeleteItemRequest")(
      test("AllOld → ALL_OLD") {
        val q   = DynamoDBQuery.deleteItem(table, pk).where($("v") === 1).returnValuesOnConditionCheckFailure(allOld)
        val req = AwsCodecs.toDeleteItemRequest(q.asInstanceOf[DynamoDBQuery.DeleteItem])
        assertTrue(req.returnValuesOnConditionCheckFailure() == AwsReturnValuesOnConditionCheckFailure.ALL_OLD)
      },
      test("unset → not sent") {
        val req = AwsCodecs.toDeleteItemRequest(DynamoDBQuery.DeleteItem(table, pk))
        assertTrue(req.returnValuesOnConditionCheckFailure() == null)
      }
    )
  )
}
