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
  ReturnItemCollectionMetrics => AwsItemMetrics,
  ReturnValue => AwsReturnValue
}
import zio.dynamodb.{ ReturnItemCollectionMetrics => LibMetrics, ReturnValues => LibReturnValues }
import zio.test._

/**
 * Verifies that .consistency(), .returns(), and .metrics() are correctly
 *  translated to their AWS SDK equivalents by the AwsCodecs request builders.
 */
object AwsCodecsMetadataSpec extends ZIOSpecDefault {

  private val pk    = PrimaryKey("id" -> "a")
  private val item  = Item("id" -> "a")
  private val table = "t"

  def spec = suite("AwsCodecs metadata wiring")(
    suite("toGetItemRequest — consistency")(
      test("Strong → consistentRead true") {
        val q   = DynamoDBQuery.GetItem(table, pk).consistency(ConsistencyMode.Strong)
        val req = AwsCodecs.toGetItemRequest(q.asInstanceOf[DynamoDBQuery.GetItem])
        assertTrue(req.consistentRead() == true)
      },
      test("Weak (default) → consistentRead false") {
        val req = AwsCodecs.toGetItemRequest(DynamoDBQuery.GetItem(table, pk))
        assertTrue(req.consistentRead() == false)
      }
    ),

    suite("toQueryRequest — consistency")(
      test("Strong → consistentRead true") {
        val q   = DynamoDBQuery.query(table, 10).consistency(ConsistencyMode.Strong)
        val req = AwsCodecs.toQueryRequest(q.asInstanceOf[DynamoDBQuery.Query])
        assertTrue(req.consistentRead() == true)
      },
      test("Weak (default) → consistentRead false") {
        val req = AwsCodecs.toQueryRequest(DynamoDBQuery.query(table, 10).asInstanceOf[DynamoDBQuery.Query])
        assertTrue(req.consistentRead() == false)
      }
    ),

    suite("toScanRequest — consistency")(
      test("Strong → consistentRead true") {
        val q   = DynamoDBQuery.scan(table, 10).consistency(ConsistencyMode.Strong)
        val req = AwsCodecs.toScanRequest(q.asInstanceOf[DynamoDBQuery.Scan])
        assertTrue(req.consistentRead() == true)
      },
      test("Weak (default) → consistentRead false") {
        val req = AwsCodecs.toScanRequest(DynamoDBQuery.scan(table, 10).asInstanceOf[DynamoDBQuery.Scan])
        assertTrue(req.consistentRead() == false)
      }
    ),

    suite("toPutItemRequest — returnValues")(
      test("AllOld → ALL_OLD") {
        val q   = DynamoDBQuery.PutItem(table, item).returns(LibReturnValues.AllOld)
        val req = AwsCodecs.toPutItemRequest(q.asInstanceOf[DynamoDBQuery.PutItem])
        assertTrue(req.returnValues() == AwsReturnValue.ALL_OLD)
      },
      test("None (default) → NONE") {
        val req = AwsCodecs.toPutItemRequest(DynamoDBQuery.PutItem(table, item))
        assertTrue(req.returnValues() == AwsReturnValue.NONE)
      }
    ),

    suite("toDeleteItemRequest — returnValues")(
      test("AllOld → ALL_OLD") {
        val q   = DynamoDBQuery.DeleteItem(table, pk).returns(LibReturnValues.AllOld)
        val req = AwsCodecs.toDeleteItemRequest(q.asInstanceOf[DynamoDBQuery.DeleteItem])
        assertTrue(req.returnValues() == AwsReturnValue.ALL_OLD)
      },
      test("None (default) → NONE") {
        val req = AwsCodecs.toDeleteItemRequest(DynamoDBQuery.DeleteItem(table, pk))
        assertTrue(req.returnValues() == AwsReturnValue.NONE)
      }
    ),

    suite("toPutItemRequest — itemMetrics")(
      test("Size → SIZE") {
        val q   = DynamoDBQuery.PutItem(table, item).metrics(LibMetrics.Size)
        val req = AwsCodecs.toPutItemRequest(q.asInstanceOf[DynamoDBQuery.PutItem])
        assertTrue(req.returnItemCollectionMetrics() == AwsItemMetrics.SIZE)
      },
      test("None (default) → NONE") {
        val req = AwsCodecs.toPutItemRequest(DynamoDBQuery.PutItem(table, item))
        assertTrue(req.returnItemCollectionMetrics() == AwsItemMetrics.NONE)
      }
    ),

    suite("toDeleteItemRequest — itemMetrics")(
      test("Size → SIZE") {
        val q   = DynamoDBQuery.DeleteItem(table, pk).metrics(LibMetrics.Size)
        val req = AwsCodecs.toDeleteItemRequest(q.asInstanceOf[DynamoDBQuery.DeleteItem])
        assertTrue(req.returnItemCollectionMetrics() == AwsItemMetrics.SIZE)
      },
      test("None (default) → NONE") {
        val req = AwsCodecs.toDeleteItemRequest(DynamoDBQuery.DeleteItem(table, pk))
        assertTrue(req.returnItemCollectionMetrics() == AwsItemMetrics.NONE)
      }
    ),

    suite("toBatchWriteItemRequest — itemMetrics")(
      test("Size → SIZE") {
        val q   = DynamoDBQuery
          .batchWriteItem(List(item))(i => DynamoDBQuery.PutItem(table, i))
          .metrics(LibMetrics.Size)
        val req = AwsCodecs.toBatchWriteItemRequest(q.asInstanceOf[DynamoDBQuery.BatchWriteItem])
        assertTrue(req.returnItemCollectionMetrics() == AwsItemMetrics.SIZE)
      },
      test("None (default) → NONE") {
        val q   = DynamoDBQuery.batchWriteItem(List(item))(i => DynamoDBQuery.PutItem(table, i))
        val req = AwsCodecs.toBatchWriteItemRequest(q)
        assertTrue(req.returnItemCollectionMetrics() == AwsItemMetrics.NONE)
      }
    )
  )
}
