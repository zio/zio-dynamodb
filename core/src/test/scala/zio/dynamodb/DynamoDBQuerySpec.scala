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
import zio.dynamodb.DynamoDBQuery.{ BatchWriteItem, DeleteItem, GetItem, PutItem, UpdateItem }
import zio.test._
import zio.test.Assertion._
import zio.dynamodb.DynamoDBQuery.CreateTable

object DynamoDBQuerySpec extends ZIOSpecDefault {

  private val table1 = "table-1"
  private val table2 = "table-2"

  private val item1 = Item("id" -> "a", "v" -> 1)
  private val item2 = Item("id" -> "b", "v" -> 2)
  private val item3 = Item("id" -> "c", "v" -> 3)

  private val key1 = PrimaryKey("id" -> "a")
  private val key2 = PrimaryKey("id" -> "b")

  private def runFailure(q: DynamoDBQuery[_, _]): Throwable =
    scala.util.Try(DummyIOInterpreter.run(q).unsafeRun()).failed.get

  private def failsAsUnsupported(modifier: String): Assertion[Any] =
    isSubtype[DynamoDBQuery.Fail](
      hasField(
        "error",
        _.error(),
        isSubtype[DynamoDBError.QueryBuilderError.UnsupportedModifier](
          hasField("message", _.message, containsString(s"'.$modifier' has no effect on"))
        )
      )
    )

  private val unsupportedModifierSuite = suite("unsupported modifiers")(
    test("an existing Fail keeps its own error") {
      val first: DynamoDBError = DynamoDBError.QueryBuilderError.UnsupportedModifier("first")
      assert(DynamoDBQuery.fail(first).capacity(total).withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[DynamoDBQuery.Fail](hasField[DynamoDBQuery.Fail, DynamoDBError]("error", _.error(), equalTo(first)))
      )
    },
    test("a Succeed is left unchanged, since it sends no request") {
      val q = DynamoDBQuery.succeed(1)
      assertTrue(q.capacity(total) eq q)
    },
    test("on a zipPar, only the leg the modifier doesn't apply to fails") {
      val createTable =
        DynamoDBQuery.createTable(table1, KeySchema("id"), AttributeDefinition.attrDefnString("id"))(
          BillingMode.PayPerRequest
        )
      val zipped      = (GetItem(table1, pk) zipPar createTable).withRetryPolicy(RetryPolicy.NoRetry)
      assert(zipped)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], Any]("left", _.left, isSubtype[GetItem](anything)) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], Any]("right", _.right, failsAsUnsupported("withRetryPolicy"))
        )
      )
    },
    test("withClientRequestToken reaches a transaction through map") {
      val tx = DynamoDBQuery.transactWriteItems(PutItem(table1, item1)).map(identity).withClientRequestToken("tok")
      assert(tx)(
        isSubtype[DynamoDBQuery.Map[_, _]](
          hasField[DynamoDBQuery.Map[_, _], Any](
            "query",
            _.query,
            isSubtype[DynamoDBQuery.TransactWriteItems](
              hasField("clientRequestToken", _.clientRequestToken, isSome(equalTo("tok")))
            )
          )
        )
      )
    },
    test("withClientRequestToken on a zipPar fails rather than sharing one token between transactions") {
      val zipped = DynamoDBQuery.transactWriteItems(PutItem(table1, item1)) zipPar
        DynamoDBQuery.transactWriteItems(PutItem(table2, item1))
      assert(zipped.withClientRequestToken("tok"))(
        isSubtype[DynamoDBQuery.Fail](
          hasField(
            "error",
            _.error(),
            isSubtype[DynamoDBError.QueryBuilderError.UnsupportedModifier](
              hasField("message", _.message, containsString("a token identifies a single transaction"))
            )
          )
        )
      )
    },
    test("withClientRequestToken fails with UnsupportedModifier outside a transaction") {
      assert(GetItem(table1, pk).withClientRequestToken("tok"))(failsAsUnsupported("withClientRequestToken"))
    },
    test("batchGetItem builds, then fails when run, for a get with a modifier it doesn't support") {
      val batch =
        DynamoDBQuery.batchGetItem(List("a"))(id => DynamoDBQuery.getItem(table1, PrimaryKey("id" -> id)).segment(1, 2))
      assert(runFailure(batch))(isSubtype[DynamoDBError.QueryBuilderError.UnsupportedModifier](anything))
    },
    test("batchGetItem fails with UnbatchableQuery when run, for a callback that doesn't return a getItem") {
      val batch =
        DynamoDBQuery.batchGetItem(List("a"))(id =>
          DynamoDBQuery.getItem(table1, PrimaryKey("id" -> id)).map(_.isDefined)
        )
      assert(runFailure(batch))(
        isSubtype[DynamoDBError.QueryBuilderError.UnbatchableQuery](
          hasField("message", _.message, containsString("must return a getItem"))
        )
      )
    },
    test("an item's capacity, metrics or retry policy inside batchWriteItem fails the batch when run") {
      def failsOn(
        modifier: String
      )(f: ItemWriteBuilder[Any, Unit, WriteOp.Put] => ItemWriteBuilder[Any, Unit, WriteOp.Put]) =
        assert(runFailure(DynamoDBQuery.batchWriteItem(List(item1))(i => f(DynamoDBQuery.putItem(table1, i)))))(
          isSubtype[DynamoDBError.QueryBuilderError.UnsupportedModifier](
            hasField(
              "message",
              _.message,
              containsString(s"'.$modifier' has no effect on an item inside batchWriteItem")
            )
          )
        )
      failsOn("capacity")(_.capacity(total)) &&
      failsOn("metrics")(_.metrics(sizeMetrics)) &&
      failsOn("withRetryPolicy")(_.withRetryPolicy(RetryPolicy.NoRetry))
    },
    test("an item's capacity or retry policy inside batchGetItem fails the batch when run") {
      def failsOn(modifier: String)(f: GetItem => GetItem) =
        assert(runFailure(DynamoDBQuery.batchGetItem(List("a"))(id => f(GetItem(table1, PrimaryKey("id" -> id))))))(
          isSubtype[DynamoDBError.QueryBuilderError.UnsupportedModifier](
            hasField("message", _.message, containsString(s"'.$modifier' has no effect on an item inside batchGetItem"))
          )
        )
      failsOn("capacity")(_.copy(capacity = total)) &&
      failsOn("withRetryPolicy")(_.copy(retryPolicy = Some(RetryPolicy.NoRetry)))
    },
    test("a get's consistency is carried to its table") {
      val batch = DynamoDBQuery
        .batchGetItem(List("a", "b"))(id => GetItem(table1, PrimaryKey("id" -> id)).copy(consistency = strong))
        .asInstanceOf[DynamoDBQuery.BatchGetItem]
        .addAll(GetItem(table2, PrimaryKey("id" -> "c")))
      assert(batch)(
        isSubtype[DynamoDBQuery.BatchGetItem](
          hasField[DynamoDBQuery.BatchGetItem, Option[ConsistencyMode]](
            "table1 consistency",
            _.requestItems.get(table1).map(_.consistency),
            isSome(equalTo(strong))
          ) &&
            hasField[DynamoDBQuery.BatchGetItem, Option[ConsistencyMode]](
              "table2 consistency",
              _.requestItems.get(table2).map(_.consistency),
              isSome(equalTo(ConsistencyMode.Weak: ConsistencyMode))
            )
        )
      ) && assert(batch.itemError)(isNone)
    },
    test("gets on one table with different consistency fail the batch with UnbatchableQuery when run") {
      val batch = DynamoDBQuery.batchGetItem(List("a", "b")) { id =>
        val get = GetItem(table1, PrimaryKey("id" -> id))
        if (id == "a") get.copy(consistency = strong) else get
      }
      assert(runFailure(batch))(
        isSubtype[DynamoDBError.QueryBuilderError.UnbatchableQuery](
          hasField("message", _.message, containsString(s"gets on table '$table1' ask for different consistency"))
        )
      )
    },
    test("the first item problem is the one reported") {
      val batch = DynamoDBQuery.batchWriteItem(List(1, 2)) { n =>
        if (n == 1) DynamoDBQuery.putItem(table1, item1).metrics(sizeMetrics)
        else DynamoDBQuery.putItem(table1, item1).capacity(total)
      }
      assert(runFailure(batch))(hasField("message", _.getMessage, containsString("'.metrics'")))
    },
    test("an item's retry policy is no longer copied onto the batch") {
      val batch = DynamoDBQuery.batchWriteItem(List(item1))(i =>
        DynamoDBQuery.putItem(table1, i).withRetryPolicy(RetryPolicy.NoRetry)
      )
      assert(batch)(isSubtype[DynamoDBQuery.BatchWriteItem](hasField("retryPolicy", _.retryPolicy, isNone)))
    },
    test("capacity, metrics and retry policy on the batch itself still run") {
      val write = DynamoDBQuery
        .batchWriteItem(List(item1))(DynamoDBQuery.putItem(table1, _))
        .capacity(total)
        .metrics(sizeMetrics)
        .withRetryPolicy(RetryPolicy.NoRetry)
      val get   = DynamoDBQuery
        .batchGetItem(List("a"))(id => DynamoDBQuery.getItem(table1, PrimaryKey("id" -> id)))
        .capacity(total)
        .withRetryPolicy(RetryPolicy.NoRetry)
      assert(DummyIOInterpreter.run(write).unsafeRun())(isSubtype[Batch.WriteResult.Complete](anything)) &&
      assert(DummyIOInterpreter.run(get).unsafeRun())(isSubtype[Batch.GetResult.Complete](anything))
    },
    test("sortOrder reaches both legs of a zipPar") {
      val zipped = (DynamoDBQuery.Query(table1, 10) zipPar DynamoDBQuery.Query(table2, 10)).sortOrder(ascending = false)
      assert(zipped)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], Any](
            "left",
            _.left,
            isSubtype[DynamoDBQuery.Query](hasField("ascending", _.ascending, isFalse))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], Any](
              "right",
              _.right,
              isSubtype[DynamoDBQuery.Query](hasField("ascending", _.ascending, isFalse))
            )
        )
      )
    }
  )

  def spec = suite("DynamoDBQuerySpec")(
    unsupportedModifierSuite,
    batchingSuite,
    capacitySuite,
    consistencySuite,
    returnsSuite,
    metricsSuite,
    startKeySuite,
    selectSuite,
    gsiSuite,
    lsiSuite,
    returnValuesOnConditionCheckFailureSuite,
    withRetryPolicySuite,
    withResponseRetryPolicySuite
  )

  // The public factory returns a DynamoDBQuery; these tests inspect the node it builds.
  private def batchWriteNode[A](values: Iterable[A])(
    body: A => ItemWriteBuilder[Any, Unit, _ <: WriteOp.Batchable]
  ): BatchWriteItem =
    DynamoDBQuery.batchWriteItem(values)(body).asInstanceOf[BatchWriteItem]

  private val batchingSuite = suite("batchWrite3")(
    test("empty iterable produces empty BatchWriteItem") {
      val batch = batchWriteNode(List.empty[Item])(item => DynamoDBQuery.putItem(table1, item))
      assertTrue(
        batch.requestItems.get(table1).isEmpty &&
          batch.addList == Chunk.empty
      )
    },
    test("single PutItem is added to requestItems and addList") {
      val batch = batchWriteNode(List(item1))(item => DynamoDBQuery.putItem(table1, item))
      assertTrue(
        batch.requestItems.get(table1).contains(Chunk[BatchWriteItem.Write](BatchWriteItem.Put(item1))) &&
          batch.addList == Chunk(BatchWriteItem.Put(item1))
      )
    },
    test("single DeleteItem is added to requestItems and addList") {
      val batch = batchWriteNode(List(key1))(key => DynamoDBQuery.deleteItem(table1, key))
      assertTrue(
        batch.requestItems.get(table1).contains(Chunk[BatchWriteItem.Write](BatchWriteItem.Delete(key1))) &&
          batch.addList == Chunk(BatchWriteItem.Delete(key1))
      )
    },
    test("multiple PutItems same table — all grouped under one key") {
      val batch = batchWriteNode(List(item1, item2, item3))(item => DynamoDBQuery.putItem(table1, item))
      assertTrue(
        batch.requestItems
          .get(table1)
          .contains(
            Chunk[BatchWriteItem.Write](
              BatchWriteItem.Put(item1),
              BatchWriteItem.Put(item2),
              BatchWriteItem.Put(item3)
            )
          ) &&
          batch.addList.length == 3
      )
    },
    test("multiple DeleteItems same table — all grouped under one key") {
      val batch = batchWriteNode(List(key1, key2))(key => DynamoDBQuery.deleteItem(table1, key))
      assertTrue(
        batch.requestItems
          .get(table1)
          .contains(
            Chunk[BatchWriteItem.Write](
              BatchWriteItem.Delete(key1),
              BatchWriteItem.Delete(key2)
            )
          ) &&
          batch.addList.length == 2
      )
    },
    test("mix of PutItem and DeleteItem same table — both types appear under same key") {
      val writes: List[Either[Item, PrimaryKey]] = List(Left(item1), Right(key2))
      val batch                                  = batchWriteNode(writes) {
        case Left(item) => DynamoDBQuery.putItem(table1, item)
        case Right(key) => DynamoDBQuery.deleteItem(table1, key)
      }
      assertTrue(
        batch.requestItems
          .get(table1)
          .contains(
            Chunk[BatchWriteItem.Write](
              BatchWriteItem.Put(item1),
              BatchWriteItem.Delete(key2)
            )
          ) &&
          batch.addList.length == 2
      )
    },
    test("PutItems across different tables — bucketed under separate keys") {
      val entries = List(table1 -> item1, table2 -> item2)
      val batch   = batchWriteNode(entries) { case (tbl, item) => DynamoDBQuery.putItem(tbl, item) }
      assertTrue(
        batch.requestItems.get(table1).contains(Chunk[BatchWriteItem.Write](BatchWriteItem.Put(item1))) &&
          batch.requestItems.get(table2).contains(Chunk[BatchWriteItem.Write](BatchWriteItem.Put(item2)))
      )
    },
    test("mix of PutItem and DeleteItem different tables — bucketed separately") {
      val writes: List[Either[Item, PrimaryKey]] = List(Left(item1), Right(key2))
      val batch                                  = batchWriteNode(writes) {
        case Left(item) => DynamoDBQuery.putItem(table1, item)
        case Right(key) => DynamoDBQuery.deleteItem(table2, key)
      }
      assertTrue(
        batch.requestItems.get(table1).contains(Chunk[BatchWriteItem.Write](BatchWriteItem.Put(item1))) &&
          batch.requestItems.get(table2).contains(Chunk[BatchWriteItem.Write](BatchWriteItem.Delete(key2)))
      )
    },
    test("addList preserves insertion order across mixed types") {
      val writes: List[Either[Item, PrimaryKey]] = List(Left(item1), Right(key1), Left(item2))
      val batch                                  = batchWriteNode(writes) {
        case Left(item) => DynamoDBQuery.putItem(table1, item)
        case Right(key) => DynamoDBQuery.deleteItem(table1, key)
      }
      assertTrue(
        batch.addList == Chunk(
          BatchWriteItem.Put(item1),
          BatchWriteItem.Delete(key1),
          BatchWriteItem.Put(item2)
        )
      )
    }
  )

  private val pk                                       = PrimaryKey("id" -> "a")
  private val total: ReturnConsumedCapacity            = ReturnConsumedCapacity.Total
  private val noCapacity: ReturnConsumedCapacity       = ReturnConsumedCapacity.None
  private val strong: ConsistencyMode                  = ConsistencyMode.Strong
  private val allOld: ReturnValues                     = ReturnValues.AllOld
  private val sizeMetrics: ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.Size

  private val capacitySuite = suite("capacity")(
    test("GetItem defaults to None") {
      assertTrue(GetItem(table1, pk).capacity == noCapacity)
    },
    test("PutItem defaults to None") {
      assertTrue(PutItem(table1, item1).capacity == noCapacity)
    },
    test("DeleteItem defaults to None") {
      assertTrue(DeleteItem(table1, pk).capacity == noCapacity)
    },
    test("Scan defaults to None") {
      assert(DynamoDBQuery.scan(table1, 10))(
        isSubtype[DynamoDBQuery.Scan](hasField("capacity", _.capacity, equalTo(noCapacity)))
      )
    },
    test("Query defaults to None") {
      assert(DynamoDBQuery.query(table1, 10))(
        isSubtype[DynamoDBQuery.Query](hasField("capacity", _.capacity, equalTo(noCapacity)))
      )
    },
    test("BatchGetItem defaults to None") {
      assert(DynamoDBQuery.batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id))))(
        isSubtype[DynamoDBQuery.BatchGetItem](hasField("capacity", _.capacity, equalTo(noCapacity)))
      )
    },
    test("BatchWriteItem defaults to None") {
      assert(DynamoDBQuery.batchWriteItem(List(item1))(i => DynamoDBQuery.putItem(table1, i)))(
        isSubtype[BatchWriteItem](hasField("capacity", _.capacity, equalTo(noCapacity)))
      )
    },
    test("capacity sets GetItem") {
      assert(GetItem(table1, pk).capacity(total))(
        isSubtype[GetItem](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets PutItem") {
      assert(PutItem(table1, item1).capacity(total))(
        isSubtype[PutItem](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets DeleteItem") {
      assert(DeleteItem(table1, pk).capacity(total))(
        isSubtype[DeleteItem](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets Scan") {
      assert(DynamoDBQuery.scan(table1, 10).capacity(total))(
        isSubtype[DynamoDBQuery.Scan](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets Query") {
      assert(DynamoDBQuery.query(table1, 10).capacity(total))(
        isSubtype[DynamoDBQuery.Query](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets BatchGetItem") {
      assert(DynamoDBQuery.batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id))).capacity(total))(
        isSubtype[DynamoDBQuery.BatchGetItem](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets BatchWriteItem") {
      assert(DynamoDBQuery.batchWriteItem(List(item1))(i => DynamoDBQuery.putItem(table1, i)).capacity(total))(
        isSubtype[DynamoDBQuery.BatchWriteItem](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity propagates through ZipPar to both branches") {
      val q = (GetItem(table1, pk) zipPar GetItem(table2, pk)).capacity(total)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[GetItem](hasField("capacity", _.capacity, equalTo(total)))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[GetItem](hasField("capacity", _.capacity, equalTo(total)))
            )
        )
      )
    },
    test("capacity propagates through Map") {
      val q = GetItem(table1, pk).map(identity).capacity(total)
      assert(q)(
        isSubtype[DynamoDBQuery.Map[_, _]](
          hasField("query", _.query, isSubtype[GetItem](hasField("capacity", _.capacity, equalTo(total))))
        )
      )
    },
    test("capacity fails with UnsupportedModifier on constructors without a capacity field") {
      val q = DynamoDBQuery.createTable(
        table1,
        KeySchema("id"),
        AttributeDefinition.attrDefnString("id")
      )(BillingMode.PayPerRequest)
      assert(q.capacity(total))(failsAsUnsupported("capacity"))
    }
  )

  private val consistencySuite = suite("consistency")(
    test("GetItem defaults to Weak") {
      assertTrue(GetItem(table1, pk).consistency == ConsistencyMode.Weak)
    },
    test("Scan defaults to Weak") {
      assert(DynamoDBQuery.scan(table1, 10))(
        isSubtype[DynamoDBQuery.Scan](
          hasField("consistency", _.consistency, equalTo(ConsistencyMode.Weak: ConsistencyMode))
        )
      )
    },
    test("Query defaults to Weak") {
      assert(DynamoDBQuery.query(table1, 10))(
        isSubtype[DynamoDBQuery.Query](
          hasField("consistency", _.consistency, equalTo(ConsistencyMode.Weak: ConsistencyMode))
        )
      )
    },
    test("consistency sets GetItem") {
      assert(GetItem(table1, pk).consistency(strong))(
        isSubtype[GetItem](hasField("consistency", _.consistency, equalTo(strong)))
      )
    },
    test("consistency sets Scan") {
      assert(DynamoDBQuery.scan(table1, 10).consistency(strong))(
        isSubtype[DynamoDBQuery.Scan](hasField("consistency", _.consistency, equalTo(strong)))
      )
    },
    test("consistency sets Query") {
      assert(DynamoDBQuery.query(table1, 10).consistency(strong))(
        isSubtype[DynamoDBQuery.Query](hasField("consistency", _.consistency, equalTo(strong)))
      )
    },
    test("consistency propagates through ZipPar to both branches") {
      val q = (GetItem(table1, pk) zipPar GetItem(table2, pk)).consistency(strong)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[GetItem](hasField("consistency", _.consistency, equalTo(strong)))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[GetItem](hasField("consistency", _.consistency, equalTo(strong)))
            )
        )
      )
    },
    test("consistency fails with UnsupportedModifier on constructors without a consistency field") {
      val q = PutItem(table1, item1)
      assert(q.consistency(strong))(failsAsUnsupported("consistency"))
    }
  )

  private val returnsSuite = suite("returns")(
    test("PutItem defaults to None") {
      assertTrue(PutItem(table1, item1).returnValues == ReturnValues.None)
    },
    test("UpdateItem defaults to None") {
      val expr = UpdateExpression(ProjectionExpression.$("v").set(2))
      assertTrue(UpdateItem(table1, pk, expr).returnValues == ReturnValues.None)
    },
    test("DeleteItem defaults to None") {
      assertTrue(DeleteItem(table1, pk).returnValues == ReturnValues.None)
    },
    test("returns sets PutItem") {
      assert(PutItem(table1, item1).returns(allOld))(
        isSubtype[PutItem](hasField("returnValues", _.returnValues, equalTo(allOld)))
      )
    },
    test("returns sets DeleteItem") {
      assert(DeleteItem(table1, pk).returns(allOld))(
        isSubtype[DeleteItem](hasField("returnValues", _.returnValues, equalTo(allOld)))
      )
    },
    test("returns propagates through ZipPar") {
      val q = (PutItem(table1, item1) zipPar DeleteItem(table2, pk)).returns(allOld)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[PutItem](hasField("returnValues", _.returnValues, equalTo(allOld)))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[DeleteItem](hasField("returnValues", _.returnValues, equalTo(allOld)))
            )
        )
      )
    },
    test("returns fails with UnsupportedModifier on constructors without a returnValues field") {
      val q = GetItem(table1, pk)
      assert(q.returns(allOld))(failsAsUnsupported("returns"))
    }
  )

  private val metricsSuite = suite("metrics")(
    test("PutItem defaults to None") {
      assertTrue(PutItem(table1, item1).itemMetrics == ReturnItemCollectionMetrics.None)
    },
    test("DeleteItem defaults to None") {
      assertTrue(DeleteItem(table1, pk).itemMetrics == ReturnItemCollectionMetrics.None)
    },
    test("BatchWriteItem defaults to None") {
      assertTrue(
        batchWriteNode(List(item1))(i =>
          DynamoDBQuery.putItem(table1, i)
        ).itemMetrics == ReturnItemCollectionMetrics.None
      )
    },
    test("metrics sets PutItem") {
      assert(PutItem(table1, item1).metrics(sizeMetrics))(
        isSubtype[PutItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))
      )
    },
    test("metrics sets DeleteItem") {
      assert(DeleteItem(table1, pk).metrics(sizeMetrics))(
        isSubtype[DeleteItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))
      )
    },
    test("metrics sets BatchWriteItem") {
      assert(DynamoDBQuery.batchWriteItem(List(item1))(i => DynamoDBQuery.putItem(table1, i)).metrics(sizeMetrics))(
        isSubtype[DynamoDBQuery.BatchWriteItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))
      )
    },
    test("metrics sets TransactWriteItems") {
      assert(DynamoDBQuery.transactWriteItems(PutItem(table1, item1)).metrics(sizeMetrics))(
        isSubtype[DynamoDBQuery.TransactWriteItems](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))
      )
    },
    test("metrics propagates through ZipPar") {
      val q = (PutItem(table1, item1) zipPar DeleteItem(table2, pk)).metrics(sizeMetrics)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[PutItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[DeleteItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))
            )
        )
      )
    },
    test("metrics fails with UnsupportedModifier on constructors without an itemMetrics field") {
      val q = GetItem(table1, pk)
      assert(q.metrics(sizeMetrics))(failsAsUnsupported("metrics"))
    }
  )

  private val someKey: Option[PrimaryKey] = Some(PrimaryKey("id" -> "a"))

  private val startKeySuite = suite("startKey")(
    test("Scan defaults to None") {
      assert(DynamoDBQuery.scan(table1, 10))(
        isSubtype[DynamoDBQuery.Scan](
          hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(None: Option[PrimaryKey]))
        )
      )
    },
    test("Query defaults to None") {
      assert(DynamoDBQuery.query(table1, 10))(
        isSubtype[DynamoDBQuery.Query](
          hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(None: Option[PrimaryKey]))
        )
      )
    },
    test("startKey sets Scan") {
      assert(DynamoDBQuery.scan(table1, 10).startKey(someKey))(
        isSubtype[DynamoDBQuery.Scan](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
      )
    },
    test("startKey sets Query") {
      assert(DynamoDBQuery.query(table1, 10).startKey(someKey))(
        isSubtype[DynamoDBQuery.Query](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
      )
    },
    test("startKey(None) resets exclusiveStartKey on Scan") {
      assert(DynamoDBQuery.scan(table1, 10).startKey(someKey).startKey(None))(
        isSubtype[DynamoDBQuery.Scan](
          hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(None: Option[PrimaryKey]))
        )
      )
    },
    test("startKey propagates through ZipPar to both branches") {
      val q = (DynamoDBQuery.scan(table1, 10) zipPar DynamoDBQuery.scan(table2, 10)).startKey(someKey)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[DynamoDBQuery.Scan](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[DynamoDBQuery.Scan](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
            )
        )
      )
    },
    test("startKey propagates through Map") {
      val q = DynamoDBQuery.scan(table1, 10).map(identity).startKey(someKey)
      assert(q)(
        isSubtype[DynamoDBQuery.Map[_, _]](
          hasField(
            "query",
            _.query,
            isSubtype[DynamoDBQuery.Scan](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
          )
        )
      )
    },
    test("startKey fails with UnsupportedModifier on constructors without an exclusiveStartKey field") {
      val q = GetItem(table1, pk)
      assert(q.startKey(someKey))(failsAsUnsupported("startKey"))
    }
  )

  private val selectSuite = suite("select")(
    test("selectCount sets Select.Count on Scan") {
      assert(DynamoDBQuery.scan(table1, 10).selectCount)(
        isSubtype[DynamoDBQuery.Scan](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
      )
    },
    test("selectCount sets Select.Count on Query") {
      assert(DynamoDBQuery.query(table1, 10).selectCount)(
        isSubtype[DynamoDBQuery.Query](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
      )
    },
    test("selectAllAttributes sets Select.AllAttributes on Scan") {
      assert(DynamoDBQuery.scan(table1, 10).selectAllAttributes)(
        isSubtype[DynamoDBQuery.Scan](
          hasField("select", _.select, equalTo(Some(Select.AllAttributes): Option[Select]))
        )
      )
    },
    test("selectAllProjectedAttributes sets Select.AllProjectedAttributes on Query") {
      assert(DynamoDBQuery.query(table1, 10).selectAllProjectedAttributes)(
        isSubtype[DynamoDBQuery.Query](
          hasField("select", _.select, equalTo(Some(Select.AllProjectedAttributes): Option[Select]))
        )
      )
    },
    test("selectSpecificAttributes sets Select.SpecificAttributes on Scan") {
      assert(DynamoDBQuery.scan(table1, 10).selectSpecificAttributes)(
        isSubtype[DynamoDBQuery.Scan](
          hasField("select", _.select, equalTo(Some(Select.SpecificAttributes): Option[Select]))
        )
      )
    },
    test("select propagates through ZipPar to both branches") {
      val q = (DynamoDBQuery.scan(table1, 10) zipPar DynamoDBQuery.scan(table2, 10)).selectCount
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[DynamoDBQuery.Scan](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[DynamoDBQuery.Scan](
                hasField("select", _.select, equalTo(Some(Select.Count): Option[Select]))
              )
            )
        )
      )
    },
    test("select propagates through Map") {
      val q = DynamoDBQuery.scan(table1, 10).map(identity).selectCount
      assert(q)(
        isSubtype[DynamoDBQuery.Map[_, _]](
          hasField(
            "query",
            _.query,
            isSubtype[DynamoDBQuery.Scan](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
          )
        )
      )
    },
    test("select fails with UnsupportedModifier on non-scan/query constructors, naming the method called") {
      val q = GetItem(table1, key1)
      assert(q.selectCount)(failsAsUnsupported("selectCount"))
    }
  )

  private val createTableBase: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.createTable(
      table1,
      KeySchema("id"),
      AttributeDefinition.attrDefnString("id")
    )(BillingMode.PayPerRequest)

  private val createTableComposite: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.createTable(
      table1,
      KeySchema("id", "year"),
      AttributeDefinition.attrDefnString("id"),
      AttributeDefinition.attrDefnString("year")
    )(BillingMode.PayPerRequest)

  private val gsiSuite = suite("gsi")(
    test("gsi with throughput adds GlobalSecondaryIndex to CreateTable") {
      val gsi =
        GlobalSecondaryIndex("score-index", KeySchema("score"), ProjectionType.All, Some(ProvisionedThroughput(5L, 5L)))
      assert(createTableBase.gsi("score-index", KeySchema("score"), ProjectionType.All, 5L, 5L))(
        isSubtype[CreateTable](hasField("globalSecondaryIndexes", _.globalSecondaryIndexes, equalTo(Set(gsi))))
      )
    },
    test("gsi without throughput adds GlobalSecondaryIndex with no provisionedThroughput") {
      val gsi = GlobalSecondaryIndex("score-index", KeySchema("score"), ProjectionType.KeysOnly, None)
      assert(createTableBase.gsi("score-index", KeySchema("score"), ProjectionType.KeysOnly))(
        isSubtype[CreateTable](hasField("globalSecondaryIndexes", _.globalSecondaryIndexes, equalTo(Set(gsi))))
      )
    },
    test("multiple gsi calls accumulate indexes in the Set") {
      val q = createTableBase
        .gsi("gsi-a", KeySchema("score"), ProjectionType.All)
        .gsi("gsi-b", KeySchema("name"), ProjectionType.KeysOnly)
      assert(q)(
        isSubtype[CreateTable](hasField("globalSecondaryIndexes", _.globalSecondaryIndexes.size, equalTo(2)))
      )
    },
    test("gsi fails with UnsupportedModifier on non-CreateTable queries") {
      val q = GetItem(table1, key1)
      assert(q.gsi("score-index", KeySchema("score"), ProjectionType.All))(failsAsUnsupported("gsi"))
    }
  )

  private val lsiSuite = suite("lsi")(
    test("lsi adds LocalSecondaryIndex to CreateTable") {
      val lsi = LocalSecondaryIndex("score-index", KeySchema("id", "score"), ProjectionType.All)
      assert(createTableComposite.lsi("score-index", KeySchema("id", "score"), ProjectionType.All))(
        isSubtype[CreateTable](hasField("localSecondaryIndexes", _.localSecondaryIndexes, equalTo(Set(lsi))))
      )
    },
    test("lsi defaults to ProjectionType.All") {
      val lsi = LocalSecondaryIndex("score-index", KeySchema("id", "score"), ProjectionType.All)
      assert(createTableComposite.lsi("score-index", KeySchema("id", "score")))(
        isSubtype[CreateTable](hasField("localSecondaryIndexes", _.localSecondaryIndexes, equalTo(Set(lsi))))
      )
    },
    test("multiple lsi calls accumulate indexes in the Set") {
      val q = createTableComposite
        .lsi("lsi-score", KeySchema("id", "score"), ProjectionType.All)
        .lsi("lsi-rank", KeySchema("id", "rank"), ProjectionType.KeysOnly)
      assert(q)(
        isSubtype[CreateTable](hasField("localSecondaryIndexes", _.localSecondaryIndexes.size, equalTo(2)))
      )
    },
    test("lsi fails with UnsupportedModifier on non-CreateTable queries") {
      val q = GetItem(table1, key1)
      assert(q.lsi("score-index", KeySchema("id", "score")))(failsAsUnsupported("lsi"))
    }
  )

  private val allOldRVOCCF: ReturnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.AllOld
  private val noRetry: RetryPolicy                              = RetryPolicy.NoRetry

  private val returnValuesOnConditionCheckFailureSuite = suite("returnValuesOnConditionCheckFailure")(
    test("sets PutItem") {
      assert(PutItem(table1, item1).returnValuesOnConditionCheckFailure(allOldRVOCCF))(
        isSubtype[PutItem](
          hasField(
            "returnValuesOnConditionCheckFailure",
            _.returnValuesOnConditionCheckFailure,
            equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
          )
        )
      )
    },
    test("sets UpdateItem") {
      val expr = UpdateExpression(ProjectionExpression.$("v").set(2))
      assert(UpdateItem(table1, pk, expr).returnValuesOnConditionCheckFailure(allOldRVOCCF))(
        isSubtype[UpdateItem](
          hasField(
            "returnValuesOnConditionCheckFailure",
            _.returnValuesOnConditionCheckFailure,
            equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
          )
        )
      )
    },
    test("sets DeleteItem") {
      assert(DeleteItem(table1, pk).returnValuesOnConditionCheckFailure(allOldRVOCCF))(
        isSubtype[DeleteItem](
          hasField(
            "returnValuesOnConditionCheckFailure",
            _.returnValuesOnConditionCheckFailure,
            equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
          )
        )
      )
    },
    test("sets ConditionCheck") {
      val cc = DynamoDBQuery.ConditionCheck(table1, pk, ProjectionExpression.$("id") === "a")
      assert(cc.returnValuesOnConditionCheckFailure(allOldRVOCCF))(
        isSubtype[DynamoDBQuery.ConditionCheck](
          hasField(
            "returnValuesOnConditionCheckFailure",
            _.returnValuesOnConditionCheckFailure,
            equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
          )
        )
      )
    },
    test("propagates through ZipPar to both branches") {
      val q = (PutItem(table1, item1) zipPar DeleteItem(table2, pk)).returnValuesOnConditionCheckFailure(allOldRVOCCF)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[PutItem](
              hasField(
                "returnValuesOnConditionCheckFailure",
                _.returnValuesOnConditionCheckFailure,
                equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
              )
            )
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[DeleteItem](
                hasField(
                  "returnValuesOnConditionCheckFailure",
                  _.returnValuesOnConditionCheckFailure,
                  equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
                )
              )
            )
        )
      )
    },
    test("propagates through Map") {
      val q = PutItem(table1, item1).map(identity).returnValuesOnConditionCheckFailure(allOldRVOCCF)
      assert(q)(
        isSubtype[DynamoDBQuery.Map[_, _]](
          hasField(
            "query",
            _.query,
            isSubtype[PutItem](
              hasField(
                "returnValuesOnConditionCheckFailure",
                _.returnValuesOnConditionCheckFailure,
                equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
              )
            )
          )
        )
      )
    },
    test("propagates through Absolve") {
      val inner   = PutItem(table1, item1)
      val absolve = DynamoDBQuery.Absolve(inner.map(r => Right(r): Either[DynamoDBError.ItemError, Option[Item]]))
      val q       = absolve.returnValuesOnConditionCheckFailure(allOldRVOCCF)
      assert(q)(
        isSubtype[DynamoDBQuery.Absolve[_, _]](
          hasField(
            "query",
            _.query,
            isSubtype[DynamoDBQuery.Map[_, _]](
              hasField(
                "query",
                _.query,
                isSubtype[PutItem](
                  hasField(
                    "returnValuesOnConditionCheckFailure",
                    _.returnValuesOnConditionCheckFailure,
                    equalTo(Some(allOldRVOCCF): Option[ReturnValuesOnConditionCheckFailure])
                  )
                )
              )
            )
          )
        )
      )
    },
    test("fails with UnsupportedModifier on constructors without the field") {
      val q = GetItem(table1, key1)
      assert(q.returnValuesOnConditionCheckFailure(allOldRVOCCF))(
        failsAsUnsupported("returnValuesOnConditionCheckFailure")
      )
    }
  )

  private val withRetryPolicySuite = suite("withRetryPolicy")(
    test("sets BatchWriteItem") {
      val q = DynamoDBQuery.batchWriteItem(List(item1))(i => DynamoDBQuery.putItem(table1, i))
      assert(q.withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[BatchWriteItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
      )
    },
    test("sets BatchGetItem") {
      val q = DynamoDBQuery.batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id)))
      assert(q.withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[DynamoDBQuery.BatchGetItem](
          hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy]))
        )
      )
    },
    test("sets PutItem") {
      assert(PutItem(table1, item1).withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[PutItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
      )
    },
    test("sets DeleteItem") {
      assert(DeleteItem(table1, pk).withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[DeleteItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
      )
    },
    test("sets GetItem") {
      assert(GetItem(table1, pk).withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[GetItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
      )
    },
    test("sets UpdateItem") {
      val expr = UpdateExpression(ProjectionExpression.$("v").set(2))
      assert(UpdateItem(table1, pk, expr).withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[UpdateItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
      )
    },
    test("sets Query") {
      assert(DynamoDBQuery.Query(table1, limit = 10).withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[DynamoDBQuery.Query](
          hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy]))
        )
      )
    },
    test("sets Scan") {
      assert(DynamoDBQuery.Scan(table1, limit = 10).withRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[DynamoDBQuery.Scan](
          hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy]))
        )
      )
    },
    test("propagates through ZipPar to both branches") {
      val q = (GetItem(table1, pk) zipPar GetItem(table2, pk)).withRetryPolicy(RetryPolicy.NoRetry)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[GetItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[GetItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
            )
        )
      )
    },
    test("propagates through Map") {
      val q = GetItem(table1, pk).map(identity).withRetryPolicy(RetryPolicy.NoRetry)
      assert(q)(
        isSubtype[DynamoDBQuery.Map[_, _]](
          hasField(
            "query",
            _.query,
            isSubtype[GetItem](hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy])))
          )
        )
      )
    },
    test("propagates through Absolve") {
      val inner   = GetItem(table1, pk)
      val absolve = DynamoDBQuery.Absolve(inner.map(r => Right(r): Either[DynamoDBError.ItemError, Option[Item]]))
      val q       = absolve.withRetryPolicy(RetryPolicy.NoRetry)
      assert(q)(
        isSubtype[DynamoDBQuery.Absolve[_, _]](
          hasField(
            "query",
            _.query,
            isSubtype[DynamoDBQuery.Map[_, _]](
              hasField(
                "query",
                _.query,
                isSubtype[GetItem](
                  hasField("retryPolicy", _.retryPolicy, equalTo(Some(noRetry): Option[RetryPolicy]))
                )
              )
            )
          )
        )
      )
    },
    test("fails with UnsupportedModifier on constructors without a retryPolicy field") {
      val q = DynamoDBQuery.createTable(
        table1,
        KeySchema("id"),
        AttributeDefinition.attrDefnString("id")
      )(BillingMode.PayPerRequest)
      assert(q.withRetryPolicy(RetryPolicy.NoRetry))(failsAsUnsupported("withRetryPolicy"))
    }
  )

  private val withResponseRetryPolicySuite = suite("withBatchUnprocessedRetryPolicy")(
    test("sets BatchWriteItem, independent of retryPolicy") {
      val q = DynamoDBQuery.batchWriteItem(List(item1))(i => DynamoDBQuery.putItem(table1, i))
      assert(q.withBatchUnprocessedRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[BatchWriteItem](
          hasField[BatchWriteItem, Option[RetryPolicy]](
            "unprocessedRetryPolicy",
            _.unprocessedRetryPolicy,
            equalTo(Some(noRetry): Option[RetryPolicy])
          ) &&
            hasField[BatchWriteItem, Option[RetryPolicy]](
              "retryPolicy",
              _.retryPolicy,
              equalTo(None: Option[RetryPolicy])
            )
        )
      )
    },
    test("sets BatchGetItem, independent of retryPolicy") {
      val q = DynamoDBQuery.batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id)))
      assert(q.withBatchUnprocessedRetryPolicy(RetryPolicy.NoRetry))(
        isSubtype[DynamoDBQuery.BatchGetItem](
          hasField[DynamoDBQuery.BatchGetItem, Option[RetryPolicy]](
            "unprocessedRetryPolicy",
            _.unprocessedRetryPolicy,
            equalTo(Some(noRetry): Option[RetryPolicy])
          ) &&
            hasField[DynamoDBQuery.BatchGetItem, Option[RetryPolicy]](
              "retryPolicy",
              _.retryPolicy,
              equalTo(None: Option[RetryPolicy])
            )
        )
      )
    },
    test("retryPolicy and unprocessedRetryPolicy can both be set, independently") {
      val q = DynamoDBQuery
        .batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id)))
        .withRetryPolicy(RetryPolicy.ExponentialBackoff(3, scala.concurrent.duration.FiniteDuration(1, "second")))
        .withBatchUnprocessedRetryPolicy(RetryPolicy.NoRetry)
      assert(q)(
        isSubtype[DynamoDBQuery.BatchGetItem](
          hasField[DynamoDBQuery.BatchGetItem, Option[RetryPolicy]](
            "unprocessedRetryPolicy",
            _.unprocessedRetryPolicy,
            equalTo(Some(noRetry): Option[RetryPolicy])
          ) &&
            hasField[DynamoDBQuery.BatchGetItem, Option[RetryPolicy]](
              "retryPolicy",
              _.retryPolicy,
              equalTo(
                Some(
                  RetryPolicy.ExponentialBackoff(3, scala.concurrent.duration.FiniteDuration(1, "second"))
                ): Option[RetryPolicy]
              )
            )
        )
      )
    },
    test("propagates through ZipPar to both branches") {
      val left  = DynamoDBQuery.batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id)))
      val right = DynamoDBQuery.batchGetItem(List("b"))(id => GetItem(table2, PrimaryKey("id" -> id)))
      val q     = (left zipPar right).withBatchUnprocessedRetryPolicy(RetryPolicy.NoRetry)
      assert(q)(
        isSubtype[DynamoDBQuery.ZipPar[_, _, _]](
          hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
            "left",
            _.left,
            isSubtype[DynamoDBQuery.BatchGetItem](
              hasField("unprocessedRetryPolicy", _.unprocessedRetryPolicy, equalTo(Some(noRetry): Option[RetryPolicy]))
            )
          ) &&
            hasField[DynamoDBQuery.ZipPar[_, _, _], DynamoDBQuery[_, _]](
              "right",
              _.right,
              isSubtype[DynamoDBQuery.BatchGetItem](
                hasField(
                  "unprocessedRetryPolicy",
                  _.unprocessedRetryPolicy,
                  equalTo(Some(noRetry): Option[RetryPolicy])
                )
              )
            )
        )
      )
    },
    test("propagates through Map") {
      val q = DynamoDBQuery
        .batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id)))
        .map(identity)
        .withBatchUnprocessedRetryPolicy(RetryPolicy.NoRetry)
      assert(q)(
        isSubtype[DynamoDBQuery.Map[_, _]](
          hasField(
            "query",
            _.query,
            isSubtype[DynamoDBQuery.BatchGetItem](
              hasField("unprocessedRetryPolicy", _.unprocessedRetryPolicy, equalTo(Some(noRetry): Option[RetryPolicy]))
            )
          )
        )
      )
    },
    test("fails with UnsupportedModifier on constructors without an unprocessedRetryPolicy field") {
      val q = GetItem(table1, pk)
      assert(q.withBatchUnprocessedRetryPolicy(RetryPolicy.NoRetry))(
        failsAsUnsupported("withBatchUnprocessedRetryPolicy")
      )
    }
  )
}
