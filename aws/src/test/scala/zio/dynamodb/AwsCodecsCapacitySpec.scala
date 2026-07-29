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

import software.amazon.awssdk.services.dynamodb.model.{ ReturnConsumedCapacity => AwsCapacity }
import zio.dynamodb.{ ReturnConsumedCapacity => LibCapacity }
import zio.test._

/**
 * Verifies that DynamoDBQuery.capacity() is correctly translated to the AWS SDK
 *  ReturnConsumedCapacity enum in each request builder.
 */
object AwsCodecsCapacitySpec extends ZIOSpecDefault {

  private val pk    = PrimaryKey("id" -> "a")
  private val item  = Item("id" -> "a")
  private val table = "t"

  def spec = suite("AwsCodecs capacity wiring")(
    suite("toGetItemRequest")(
      test("Total → TOTAL") {
        val q   = DynamoDBQuery.GetItem(table, pk).capacity(LibCapacity.Total)
        val req = AwsCodecs.toGetItemRequest(q.asInstanceOf[DynamoDBQuery.GetItem])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.TOTAL)
      },
      test("None → NONE") {
        val req = AwsCodecs.toGetItemRequest(DynamoDBQuery.GetItem(table, pk))
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.NONE)
      },
      test("Indexes → INDEXES") {
        val q   = DynamoDBQuery.GetItem(table, pk).capacity(LibCapacity.Indexes)
        val req = AwsCodecs.toGetItemRequest(q.asInstanceOf[DynamoDBQuery.GetItem])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.INDEXES)
      }
    ),

    suite("toPutItemRequest")(
      test("Total → TOTAL") {
        val q   = DynamoDBQuery.PutItem(table, item).capacity(LibCapacity.Total)
        val req = AwsCodecs.toPutItemRequest(q.asInstanceOf[DynamoDBQuery.PutItem])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.TOTAL)
      },
      test("None → NONE") {
        val req = AwsCodecs.toPutItemRequest(DynamoDBQuery.PutItem(table, item))
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.NONE)
      }
    ),

    suite("toDeleteItemRequest")(
      test("Total → TOTAL") {
        val q   = DynamoDBQuery.DeleteItem(table, pk).capacity(LibCapacity.Total)
        val req = AwsCodecs.toDeleteItemRequest(q.asInstanceOf[DynamoDBQuery.DeleteItem])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.TOTAL)
      },
      test("None → NONE") {
        val req = AwsCodecs.toDeleteItemRequest(DynamoDBQuery.DeleteItem(table, pk))
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.NONE)
      }
    ),

    suite("toScanSomeRequest")(
      test("Total → TOTAL") {
        val q   = DynamoDBQuery.scanSome(table, 10).capacity(LibCapacity.Total)
        val req = AwsCodecs.toScanSomeRequest(q.asInstanceOf[DynamoDBQuery.ScanSome])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.TOTAL)
      },
      test("None → NONE") {
        val req = AwsCodecs.toScanSomeRequest(DynamoDBQuery.scanSome(table, 10).asInstanceOf[DynamoDBQuery.ScanSome])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.NONE)
      }
    ),

    suite("toQuerySomeRequest")(
      test("Total → TOTAL") {
        val q   = DynamoDBQuery.querySome(table, 10).capacity(LibCapacity.Total)
        val req = AwsCodecs.toQuerySomeRequest(q.asInstanceOf[DynamoDBQuery.QuerySome])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.TOTAL)
      },
      test("None → NONE") {
        val req = AwsCodecs.toQuerySomeRequest(DynamoDBQuery.querySome(table, 10).asInstanceOf[DynamoDBQuery.QuerySome])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.NONE)
      }
    ),

    suite("toBatchGetItemRequest")(
      test("Total → TOTAL") {
        val q   = DynamoDBQuery
          .batchGetItem(List("a"))(id => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> id)))
          .capacity(LibCapacity.Total)
        val req = AwsCodecs.toBatchGetItemRequest(q.asInstanceOf[DynamoDBQuery.BatchGetItem])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.TOTAL)
      },
      test("None → NONE") {
        val q   = DynamoDBQuery.batchGetItem(List("a"))(id => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> id)))
        val req = AwsCodecs.toBatchGetItemRequest(q)
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.NONE)
      }
    ),

    suite("toBatchWriteItemRequest")(
      test("Total → TOTAL") {
        val q   = DynamoDBQuery
          .batchWriteItem(List(item))(i => DynamoDBQuery.PutItem(table, i))
          .capacity(LibCapacity.Total)
        val req = AwsCodecs.toBatchWriteItemRequest(q.asInstanceOf[DynamoDBQuery.BatchWriteItem])
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.TOTAL)
      },
      test("None → NONE") {
        val q   = DynamoDBQuery.batchWriteItem(List(item))(i => DynamoDBQuery.PutItem(table, i))
        val req = AwsCodecs.toBatchWriteItemRequest(q)
        assertTrue(req.returnConsumedCapacity() == AwsCapacity.NONE)
      }
    )
  )
}
