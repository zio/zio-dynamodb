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
  AttributeValue => AwsAttrValue,
  CancellationReason => AwsCancellationReason,
  ItemResponse,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  TransactionCanceledException
}
import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.{ CancellationReason, TransactionError }
import zio.dynamodb.ProjectionExpression.$
import zio.test._
import zio.test.Assertion.{ anything, equalTo, hasField, isSome, isSubtype }

import scala.collection.JavaConverters._
import scala.util.Try

object TransactionSpec extends ZIOSpecDefault {

  private val table = "t"
  private val pk    = PrimaryKey("id" -> "alice")
  private val item  = Item("id" -> "alice", "score" -> 42)

  private def awsStr(v: String) = AwsAttrValue.builder().s(v).build()
  private val awsItem           = java.util.Map.of("id", awsStr("alice"), "score", awsStr("42"))
  // Expected domain Item after decoding awsItem (score encoded as String "42" in the stub)
  private val decodedItem       = Item("id" -> "alice", "score" -> "42")

  // ---------------------------------------------------------------------------
  // Stub builder for transaction operations
  // ---------------------------------------------------------------------------

  private def txStub(
    onTransactGet: TransactGetItemsRequest => TransactGetItemsResponse = _ =>
      TransactGetItemsResponse.builder().build(),
    onTransactWrite: TransactWriteItemsRequest => TransactWriteItemsResponse = _ =>
      TransactWriteItemsResponse.builder().build()
  ): AwsDynamoDB[DummyIO] =
    new AwsDynamoDB[DummyIO] {
      def getItem(req: software.amazon.awssdk.services.dynamodb.model.GetItemRequest)               =
        DummyIO.succeed(???)
      def putItem(req: software.amazon.awssdk.services.dynamodb.model.PutItemRequest)               =
        DummyIO.succeed(???)
      def updateItem(req: software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest)         =
        DummyIO.succeed(???)
      def deleteItem(req: software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest)         =
        DummyIO.succeed(???)
      def batchGetItem(req: software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest)     =
        DummyIO.succeed(???)
      def batchWriteItem(req: software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest) =
        DummyIO.succeed(???)
      def querySome(req: software.amazon.awssdk.services.dynamodb.model.QueryRequest)               =
        DummyIO.succeed(???)
      def scanSome(req: software.amazon.awssdk.services.dynamodb.model.ScanRequest)                 =
        DummyIO.succeed(???)
      def createTable(req: software.amazon.awssdk.services.dynamodb.model.CreateTableRequest)       =
        DummyIO.succeed(???)
      def deleteTable(req: software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest)       =
        DummyIO.succeed(???)
      def describeTable(req: software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest)   =
        DummyIO.succeed(???)
      def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]         =
        DummyIO.succeed(onTransactGet(req))
      def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse]   =
        DummyIO.succeed(onTransactWrite(req))
    }

  private def run[A](q: DynamoDBQuery[_, A], client: AwsDynamoDB[DummyIO] = txStub()): Try[A] =
    Try(new DummyIOInterpreter(client).run(q).unsafeRun())

  // ---------------------------------------------------------------------------
  // Suite 1: toTransactGetItemsRequest codec
  // ---------------------------------------------------------------------------

  private val toTransactGetRequestSuite = suite("toTransactGetItemsRequest codec")(
    test("single GetItem: tableName and key are encoded") {
      var captured: TransactGetItemsRequest = null
      run(
        DynamoDBQuery.transactGetItems(DynamoDBQuery.GetItem(table, pk)),
        txStub(onTransactGet = req => { captured = req; TransactGetItemsResponse.builder().build() })
      )
      val get                               = captured.transactItems().get(0).get()
      assertTrue(
        get.tableName() == table &&
          get.key().get("id").s() == "alice"
      )
    },
    test("two GetItems are encoded in order") {
      val pk2                               = PrimaryKey("id" -> "bob")
      var captured: TransactGetItemsRequest = null
      run(
        DynamoDBQuery.transactGetItems(
          DynamoDBQuery.GetItem(table, pk),
          DynamoDBQuery.GetItem(table, pk2)
        ),
        txStub(onTransactGet = req => { captured = req; TransactGetItemsResponse.builder().build() })
      )
      val items                             = captured.transactItems().asScala.map(_.get())
      assertTrue(
        items.length == 2 &&
          items(0).key().get("id").s() == "alice" &&
          items(1).key().get("id").s() == "bob"
      )
    },
    test("GetItem with projection: projectionExpression and name aliases are set") {
      var captured: TransactGetItemsRequest = null
      run(
        DynamoDBQuery.transactGetItems(DynamoDBQuery.GetItem(table, pk, List($("score")))),
        txStub(onTransactGet = req => { captured = req; TransactGetItemsResponse.builder().build() })
      )
      val get                               = captured.transactItems().get(0).get()
      assertTrue(
        get.projectionExpression() != null &&
          get.hasExpressionAttributeNames()
      )
    },
    test("capacity(Total) sets returnConsumedCapacity on the request") {
      var captured: TransactGetItemsRequest = null
      run(
        DynamoDBQuery
          .transactGetItems(DynamoDBQuery.GetItem(table, pk))
          .capacity(ReturnConsumedCapacity.Total),
        txStub(onTransactGet = req => { captured = req; TransactGetItemsResponse.builder().build() })
      )
      assertTrue(
        captured.returnConsumedCapacity().toString == "TOTAL"
      )
    },
    test("consistency on inner GetItem is stripped (transactional isolation is always serializable)") {
      var captured: TransactGetItemsRequest = null
      run(
        DynamoDBQuery.transactGetItems(
          DynamoDBQuery.GetItem(table, pk, consistency = ConsistencyMode.Strong)
        ),
        txStub(onTransactGet = req => { captured = req; TransactGetItemsResponse.builder().build() })
      )
      // The Get wrapper inside TransactGetItem has no consistentRead field —
      // this test simply verifies encoding succeeds without error.
      assertTrue(captured.transactItems().size() == 1)
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 2: fromTransactGetItemsResponse codec
  // ---------------------------------------------------------------------------

  private val fromTransactGetResponseSuite = suite("fromTransactGetItemsResponse codec")(
    test("item present → Some(item)") {
      val resp = TransactGetItemsResponse
        .builder()
        .responses(java.util.List.of(ItemResponse.builder().item(awsItem).build()))
        .build()
      run(
        DynamoDBQuery.transactGetItems(DynamoDBQuery.GetItem(table, pk)),
        txStub(onTransactGet = _ => resp)
      ).map { results =>
        assertTrue(results.length == 1 && results(0).contains(decodedItem))
      }.get
    },
    test("empty ItemResponse → None") {
      val resp = TransactGetItemsResponse
        .builder()
        .responses(java.util.List.of(ItemResponse.builder().build()))
        .build()
      run(
        DynamoDBQuery.transactGetItems(DynamoDBQuery.GetItem(table, pk)),
        txStub(onTransactGet = _ => resp)
      ).map { results =>
        assertTrue(results.length == 1 && results(0).isEmpty)
      }.get
    },
    test("mixed responses preserve positional order: Some then None") {
      val resp = TransactGetItemsResponse
        .builder()
        .responses(
          java.util.List.of(
            ItemResponse.builder().item(awsItem).build(),
            ItemResponse.builder().build()
          )
        )
        .build()
      val q    = DynamoDBQuery.transactGetItems(
        DynamoDBQuery.GetItem(table, pk),
        DynamoDBQuery.GetItem(table, PrimaryKey("id" -> "bob"))
      )
      run(q, txStub(onTransactGet = _ => resp)).map { results =>
        assertTrue(results.length == 2 && results(0).contains(decodedItem) && results(1).isEmpty)
      }.get
    },
    test("no responses field → empty Chunk") {
      val resp = TransactGetItemsResponse.builder().build()
      run(
        DynamoDBQuery.transactGetItems(DynamoDBQuery.GetItem(table, pk)),
        txStub(onTransactGet = _ => resp)
      ).map { results =>
        assertTrue(results.isEmpty)
      }.get
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 3: toTransactWriteItemsRequest codec — all four action types
  // ---------------------------------------------------------------------------

  private val toTransactWriteRequestSuite = suite("toTransactWriteItemsRequest codec")(
    test("PutItem: tableName and item are encoded") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(DynamoDBQuery.putItem(table, item))
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val put                                 = captured.transactItems().get(0).put()
      assertTrue(put != null && put.tableName() == table && put.item().get("id").s() == "alice")
    },
    test("PutItem with conditionExpression: condition is set") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(
        DynamoDBQuery.putItem(table, item).where($("score") === 0)
      )
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val put                                 = captured.transactItems().get(0).put()
      assertTrue(put.conditionExpression() != null)
    },
    test("PutItem with returnValuesOnConditionCheckFailure: field is set") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(
        DynamoDBQuery
          .putItem(table, item)
          .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
      )
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val put                                 = captured.transactItems().get(0).put()
      assertTrue(put.returnValuesOnConditionCheckFailure().toString == "ALL_OLD")
    },
    test("UpdateItem: tableName, key, and updateExpression are encoded") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(
        DynamoDBQuery.updateItem(table, pk)($("score").set(99))
      )
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val upd                                 = captured.transactItems().get(0).update()
      assertTrue(
        upd != null &&
          upd.tableName() == table &&
          upd.key().get("id").s() == "alice" &&
          upd.updateExpression() != null
      )
    },
    test("UpdateItem with conditionExpression: condition is set") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(
        DynamoDBQuery.updateItem(table, pk)($("score").set(99)).where($("score") === 42)
      )
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val upd                                 = captured.transactItems().get(0).update()
      assertTrue(upd.conditionExpression() != null)
    },
    test("DeleteItem: tableName and key are encoded") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(DynamoDBQuery.deleteItem(table, pk))
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val del                                 = captured.transactItems().get(0).delete()
      assertTrue(del != null && del.tableName() == table && del.key().get("id").s() == "alice")
    },
    test("ConditionCheck: tableName, key and conditionExpression are encoded") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(
        DynamoDBQuery.conditionCheck(table, pk)($("score") === 42)
      )
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val cc                                  = captured.transactItems().get(0).conditionCheck()
      assertTrue(
        cc != null &&
          cc.tableName() == table &&
          cc.key().get("id").s() == "alice" &&
          cc.conditionExpression() != null
      )
    },
    test("ConditionCheck with returnValuesOnConditionCheckFailure: field is set") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(
        DynamoDBQuery
          .conditionCheck(table, pk)($("score") === 42)
          .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
      )
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val cc                                  = captured.transactItems().get(0).conditionCheck()
      assertTrue(cc.returnValuesOnConditionCheckFailure().toString == "ALL_OLD")
    },
    test("clientRequestToken is set when withClientRequestToken is called") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery
        .transactWriteItems(DynamoDBQuery.putItem(table, item))
        .withClientRequestToken("my-token-123")
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      assertTrue(captured.clientRequestToken() == "my-token-123")
    },
    test("clientRequestToken is absent when not set") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(DynamoDBQuery.putItem(table, item))
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      assertTrue(captured.clientRequestToken() == null)
    },
    test("mixed batch: all four action types encoded in order") {
      var captured: TransactWriteItemsRequest = null
      val q                                   = DynamoDBQuery.transactWriteItems(
        DynamoDBQuery.putItem(table, item),
        DynamoDBQuery.updateItem(table, pk)($("score").set(1)),
        DynamoDBQuery.deleteItem(table, pk),
        DynamoDBQuery.conditionCheck(table, pk)($("score") === 42)
      )
      run(q, txStub(onTransactWrite = req => { captured = req; TransactWriteItemsResponse.builder().build() }))
      val items                               = captured.transactItems().asScala
      assertTrue(
        items.length == 4 &&
          items(0).put() != null &&
          items(1).update() != null &&
          items(2).delete() != null &&
          items(3).conditionCheck() != null
      )
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 4: fromTransactionCanceledException codec
  // ---------------------------------------------------------------------------

  private val fromTransactionCancelledSuite = suite("fromTransactionCanceledException codec")(
    test("extracts code and message from a single reason") {
      val ex     = TransactionCanceledException
        .builder()
        .cancellationReasons(
          java.util.List.of(
            AwsCancellationReason
              .builder()
              .code("ConditionalCheckFailed")
              .message("The condition was not met")
              .build()
          )
        )
        .build()
        .asInstanceOf[TransactionCanceledException]
      val result = AwsCodecs.fromTransactionCanceledException(ex)
      assertTrue(
        result.reasons.length == 1 &&
          result.reasons(0).code == "ConditionalCheckFailed" &&
          result.reasons(0).message.contains("The condition was not met") &&
          result.reasons(0).item.isEmpty
      )
    },
    test("item is Some when cancellation reason carries item data") {
      val ex     = TransactionCanceledException
        .builder()
        .cancellationReasons(
          java.util.List.of(
            AwsCancellationReason
              .builder()
              .code("ConditionalCheckFailed")
              .item(awsItem)
              .build()
          )
        )
        .build()
        .asInstanceOf[TransactionCanceledException]
      val result = AwsCodecs.fromTransactionCanceledException(ex)
      assertTrue(result.reasons(0).item.contains(decodedItem))
    },
    test("multiple reasons are positionally preserved") {
      val ex     = TransactionCanceledException
        .builder()
        .cancellationReasons(
          java.util.List.of(
            AwsCancellationReason.builder().code("ConditionalCheckFailed").build(),
            AwsCancellationReason.builder().code("None").build()
          )
        )
        .build()
        .asInstanceOf[TransactionCanceledException]
      val result = AwsCodecs.fromTransactionCanceledException(ex)
      assertTrue(
        result.reasons.length == 2 &&
          result.reasons(0).code == "ConditionalCheckFailed" &&
          result.reasons(1).code == "None"
      )
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 5: Interpreter validation — item count bounds
  // ---------------------------------------------------------------------------

  private val validationSuite = suite("interpreter validates item count")(
    test("transactGetItems with 0 items fails with TransactionValidationError") {
      val result = run(DynamoDBQuery.transactGetItems())
      assert(result.failed.get)(isSubtype[TransactionError.TransactionValidationError](anything)) &&
      assertTrue(result.failed.get.getMessage.contains("between 1 and 100"))
    },
    test("transactGetItems with 101 items fails with TransactionValidationError") {
      val items  = (1 to 101).map(i => DynamoDBQuery.GetItem(table, PrimaryKey("id" -> i.toString)))
      val result = run(DynamoDBQuery.transactGetItems(items: _*))
      assert(result.failed.get)(isSubtype[TransactionError.TransactionValidationError](anything))
    },
    test("transactWriteItems with 0 items fails with TransactionValidationError") {
      val result = run(DynamoDBQuery.transactWriteItems())
      assert(result.failed.get)(isSubtype[TransactionError.TransactionValidationError](anything)) &&
      assertTrue(result.failed.get.getMessage.contains("between 1 and 100"))
    },
    test("transactWriteItems with 101 items fails with TransactionValidationError") {
      val writes = (1 to 101).map(i => DynamoDBQuery.putItem(table, Item("id" -> i.toString)))
      val result = run(DynamoDBQuery.transactWriteItems(writes: _*))
      assert(result.failed.get)(isSubtype[TransactionError.TransactionValidationError](anything))
    },
    test("transactGetItems with 1 item succeeds") {
      val result = run(
        DynamoDBQuery.transactGetItems(DynamoDBQuery.GetItem(table, pk)),
        txStub(onTransactGet = _ => TransactGetItemsResponse.builder().build())
      )
      assertTrue(result.isSuccess)
    },
    test("transactWriteItems with 1 item succeeds") {
      val result = run(
        DynamoDBQuery.transactWriteItems(DynamoDBQuery.putItem(table, item)),
        txStub()
      )
      assertTrue(result.isSuccess)
    },
    test("ConditionCheck as standalone query fails with TransactionValidationError") {
      val result = run(DynamoDBQuery.conditionCheck(table, pk)($("score") === 42))
      assert(result.failed.get)(isSubtype[TransactionError.TransactionValidationError](anything)) &&
      assertTrue(result.failed.get.getMessage.contains("standalone"))
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 6: Interpreter error wrapping — TransactionCanceledException
  // ---------------------------------------------------------------------------

  private val errorWrappingSuite = suite("TransactionCanceledException is mapped to TransactionCancelled")(
    test("exception thrown by client becomes a TransactionCancelled failure") {
      val ex     = TransactionCanceledException
        .builder()
        .cancellationReasons(
          java.util.List.of(
            AwsCancellationReason
              .builder()
              .code("ConditionalCheckFailed")
              .message("failed")
              .build()
          )
        )
        .build()
        .asInstanceOf[TransactionCanceledException]
      val client = txStub(onTransactWrite = _ => throw ex)
      val result = run(
        DynamoDBQuery.transactWriteItems(DynamoDBQuery.putItem(table, item)),
        client
      )
      assert(result.failed.get)(isSubtype[TransactionError.TransactionCancelled](anything)) &&
      assertTrue(result.failed.get.getMessage.contains("ConditionalCheckFailed"))
    },
    test("non-transaction exception propagates unchanged") {
      val ex     = new RuntimeException("network error")
      val client = txStub(onTransactWrite = _ => throw ex)
      val result = run(
        DynamoDBQuery.transactWriteItems(DynamoDBQuery.putItem(table, item)),
        client
      )
      assertTrue(
        result.isFailure &&
          result.failed.get.getMessage.contains("network error")
      )
    }
  )

  // ---------------------------------------------------------------------------
  // Suite 7: Builder methods
  // ---------------------------------------------------------------------------

  private val builderSuite = suite("builder methods")(
    suite("withClientRequestToken")(
      test("sets token on TransactWriteItems") {
        val q = DynamoDBQuery
          .transactWriteItems(DynamoDBQuery.putItem(table, item))
          .withClientRequestToken("tok-abc")
        assert(q)(
          isSubtype[DynamoDBQuery.TransactWriteItems](
            hasField("clientRequestToken", _.clientRequestToken, isSome(equalTo("tok-abc")))
          )
        )
      },
      test("no-op on non-TransactWriteItems query") {
        val base = DynamoDBQuery.getItem(table, pk)
        assertTrue(base.withClientRequestToken("tok").eq(base))
      }
    ),
    suite("returnValuesOnConditionCheckFailure")(
      test("propagates to PutItem") {
        val q = DynamoDBQuery
          .putItem(table, item)
          .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
        assert(q)(
          isSubtype[DynamoDBQuery.PutItem](
            hasField(
              "returnValuesOnConditionCheckFailure",
              _.returnValuesOnConditionCheckFailure,
              isSome(equalTo(ReturnValuesOnConditionCheckFailure.AllOld: ReturnValuesOnConditionCheckFailure))
            )
          )
        )
      },
      test("propagates to UpdateItem") {
        val q = DynamoDBQuery
          .UpdateItem(table, pk, UpdateExpression($("score").set(1)))
          .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
        assert(q)(
          isSubtype[DynamoDBQuery.UpdateItem](
            hasField(
              "returnValuesOnConditionCheckFailure",
              _.returnValuesOnConditionCheckFailure,
              isSome(equalTo(ReturnValuesOnConditionCheckFailure.AllOld: ReturnValuesOnConditionCheckFailure))
            )
          )
        )
      },
      test("propagates to DeleteItem") {
        val q = DynamoDBQuery
          .deleteItem(table, pk)
          .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
        assert(q)(
          isSubtype[DynamoDBQuery.DeleteItem](
            hasField(
              "returnValuesOnConditionCheckFailure",
              _.returnValuesOnConditionCheckFailure,
              isSome(equalTo(ReturnValuesOnConditionCheckFailure.AllOld: ReturnValuesOnConditionCheckFailure))
            )
          )
        )
      },
      test("propagates to ConditionCheck") {
        val q = DynamoDBQuery
          .conditionCheck(table, pk)($("score") === 42)
          .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
        assert(q)(
          isSubtype[DynamoDBQuery.ConditionCheck](
            hasField(
              "returnValuesOnConditionCheckFailure",
              _.returnValuesOnConditionCheckFailure,
              isSome(equalTo(ReturnValuesOnConditionCheckFailure.AllOld: ReturnValuesOnConditionCheckFailure))
            )
          )
        )
      }
    ),
    suite("capacity")(
      test("propagates to TransactGetItems") {
        val q = DynamoDBQuery
          .transactGetItems(DynamoDBQuery.GetItem(table, pk))
          .capacity(ReturnConsumedCapacity.Total)
        assert(q)(
          isSubtype[DynamoDBQuery.TransactGetItems](
            hasField("capacity", _.capacity, equalTo(ReturnConsumedCapacity.Total: ReturnConsumedCapacity))
          )
        )
      },
      test("propagates to TransactWriteItems") {
        val q = DynamoDBQuery
          .transactWriteItems(DynamoDBQuery.putItem(table, item))
          .capacity(ReturnConsumedCapacity.Total)
        assert(q)(
          isSubtype[DynamoDBQuery.TransactWriteItems](
            hasField("capacity", _.capacity, equalTo(ReturnConsumedCapacity.Total: ReturnConsumedCapacity))
          )
        )
      }
    )
  )

  def spec = suite("TransactionSpec")(
    toTransactGetRequestSuite,
    fromTransactGetResponseSuite,
    toTransactWriteRequestSuite,
    fromTransactionCancelledSuite,
    validationSuite,
    errorWrappingSuite,
    builderSuite
  )
}
