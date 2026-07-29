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

  def spec = suite("DynamoDBQuerySpec")(
    batchingSuite,
    capacitySuite,
    consistencySuite,
    returnsSuite,
    metricsSuite,
    startKeySuite,
    selectSuite,
    gsiSuite,
    lsiSuite
  )

  private val batchingSuite = suite("batchWrite3")(
    test("empty iterable produces empty BatchWriteItem") {
      val batch = DynamoDBQuery.batchWriteItem(List.empty[Item])(item => PutItem(table1, item))
      assertTrue(
        batch.requestItems.get(table1).isEmpty &&
          batch.addList == Chunk.empty
      )
    },
    test("single PutItem is added to requestItems and addList") {
      val batch = DynamoDBQuery.batchWriteItem(List(item1))(item => PutItem(table1, item))
      assertTrue(
        batch.requestItems.get(table1).contains(Set[BatchWriteItem.Write](BatchWriteItem.Put(item1))) &&
          batch.addList == Chunk(BatchWriteItem.Put(item1))
      )
    },
    test("single DeleteItem is added to requestItems and addList") {
      val batch = DynamoDBQuery.batchWriteItem(List(key1))(key => DeleteItem(table1, key))
      assertTrue(
        batch.requestItems.get(table1).contains(Set[BatchWriteItem.Write](BatchWriteItem.Delete(key1))) &&
          batch.addList == Chunk(BatchWriteItem.Delete(key1))
      )
    },
    test("multiple PutItems same table — all grouped under one key") {
      val batch = DynamoDBQuery.batchWriteItem(List(item1, item2, item3))(item => PutItem(table1, item))
      assertTrue(
        batch.requestItems
          .get(table1)
          .contains(
            Set[BatchWriteItem.Write](
              BatchWriteItem.Put(item1),
              BatchWriteItem.Put(item2),
              BatchWriteItem.Put(item3)
            )
          ) &&
          batch.addList.length == 3
      )
    },
    test("multiple DeleteItems same table — all grouped under one key") {
      val batch = DynamoDBQuery.batchWriteItem(List(key1, key2))(key => DeleteItem(table1, key))
      assertTrue(
        batch.requestItems
          .get(table1)
          .contains(
            Set[BatchWriteItem.Write](
              BatchWriteItem.Delete(key1),
              BatchWriteItem.Delete(key2)
            )
          ) &&
          batch.addList.length == 2
      )
    },
    test("mix of PutItem and DeleteItem same table — both types appear under same key") {
      val writes: List[Either[Item, PrimaryKey]] = List(Left(item1), Right(key2))
      val batch                                  = DynamoDBQuery.batchWriteItem(writes) {
        case Left(item) => PutItem(table1, item)
        case Right(key) => DeleteItem(table1, key)
      }
      assertTrue(
        batch.requestItems
          .get(table1)
          .contains(
            Set[BatchWriteItem.Write](
              BatchWriteItem.Put(item1),
              BatchWriteItem.Delete(key2)
            )
          ) &&
          batch.addList.length == 2
      )
    },
    test("PutItems across different tables — bucketed under separate keys") {
      val entries = List(table1 -> item1, table2 -> item2)
      val batch   = DynamoDBQuery.batchWriteItem(entries) { case (tbl, item) => PutItem(tbl, item) }
      assertTrue(
        batch.requestItems.get(table1).contains(Set[BatchWriteItem.Write](BatchWriteItem.Put(item1))) &&
          batch.requestItems.get(table2).contains(Set[BatchWriteItem.Write](BatchWriteItem.Put(item2)))
      )
    },
    test("mix of PutItem and DeleteItem different tables — bucketed separately") {
      val writes: List[Either[Item, PrimaryKey]] = List(Left(item1), Right(key2))
      val batch                                  = DynamoDBQuery.batchWriteItem(writes) {
        case Left(item) => PutItem(table1, item)
        case Right(key) => DeleteItem(table2, key)
      }
      assertTrue(
        batch.requestItems.get(table1).contains(Set[BatchWriteItem.Write](BatchWriteItem.Put(item1))) &&
          batch.requestItems.get(table2).contains(Set[BatchWriteItem.Write](BatchWriteItem.Delete(key2)))
      )
    },
    test("addList preserves insertion order across mixed types") {
      val writes: List[Either[Item, PrimaryKey]] = List(Left(item1), Right(key1), Left(item2))
      val batch                                  = DynamoDBQuery.batchWriteItem(writes) {
        case Left(item) => PutItem(table1, item)
        case Right(key) => DeleteItem(table1, key)
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
    test("ScanSome defaults to None") {
      assert(DynamoDBQuery.scanSome(table1, 10))(
        isSubtype[DynamoDBQuery.ScanSome](hasField("capacity", _.capacity, equalTo(noCapacity)))
      )
    },
    test("QuerySome defaults to None") {
      assert(DynamoDBQuery.querySome(table1, 10))(
        isSubtype[DynamoDBQuery.QuerySome](hasField("capacity", _.capacity, equalTo(noCapacity)))
      )
    },
    test("BatchGetItem defaults to None") {
      assertTrue(
        DynamoDBQuery.batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id))).capacity == noCapacity
      )
    },
    test("BatchWriteItem defaults to None") {
      assertTrue(DynamoDBQuery.batchWriteItem(List(item1))(i => PutItem(table1, i)).capacity == noCapacity)
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
    test("capacity sets ScanSome") {
      assert(DynamoDBQuery.scanSome(table1, 10).capacity(total))(
        isSubtype[DynamoDBQuery.ScanSome](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets QuerySome") {
      assert(DynamoDBQuery.querySome(table1, 10).capacity(total))(
        isSubtype[DynamoDBQuery.QuerySome](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets BatchGetItem") {
      assert(DynamoDBQuery.batchGetItem(List("a"))(id => GetItem(table1, PrimaryKey("id" -> id))).capacity(total))(
        isSubtype[DynamoDBQuery.BatchGetItem](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity sets BatchWriteItem") {
      assert(DynamoDBQuery.batchWriteItem(List(item1))(i => PutItem(table1, i)).capacity(total))(
        isSubtype[DynamoDBQuery.BatchWriteItem](hasField("capacity", _.capacity, equalTo(total)))
      )
    },
    test("capacity propagates through ZipPar to both branches") {
      val q = (GetItem(table1, pk) zipPar GetItem(table2, pk)).capacity(total)
      q match {
        case DynamoDBQuery.ZipPar(l, r, _) =>
          assert(l)(isSubtype[GetItem](hasField("capacity", _.capacity, equalTo(total)))) &&
          assert(r)(isSubtype[GetItem](hasField("capacity", _.capacity, equalTo(total))))
        case _                             => assertTrue(false)
      }
    },
    test("capacity propagates through Map") {
      val q = GetItem(table1, pk).map(identity).capacity(total)
      q match {
        case DynamoDBQuery.Map(inner, _) =>
          assert(inner)(isSubtype[GetItem](hasField("capacity", _.capacity, equalTo(total))))
        case _                           => assertTrue(false)
      }
    },
    test("capacity is a no-op for constructors without a capacity field") {
      val q = DynamoDBQuery.createTable(
        table1,
        KeySchema("id"),
        NonEmptySet(AttributeDefinition.attrDefnString("id")),
        BillingMode.PayPerRequest
      )
      assertTrue(q.capacity(total) eq q)
    }
  )

  private val consistencySuite = suite("consistency")(
    test("GetItem defaults to Weak") {
      assertTrue(GetItem(table1, pk).consistency == ConsistencyMode.Weak)
    },
    test("ScanSome defaults to Weak") {
      assert(DynamoDBQuery.scanSome(table1, 10))(
        isSubtype[DynamoDBQuery.ScanSome](
          hasField("consistency", _.consistency, equalTo(ConsistencyMode.Weak: ConsistencyMode))
        )
      )
    },
    test("QuerySome defaults to Weak") {
      assert(DynamoDBQuery.querySome(table1, 10))(
        isSubtype[DynamoDBQuery.QuerySome](
          hasField("consistency", _.consistency, equalTo(ConsistencyMode.Weak: ConsistencyMode))
        )
      )
    },
    test("consistency sets GetItem") {
      assert(GetItem(table1, pk).consistency(strong))(
        isSubtype[GetItem](hasField("consistency", _.consistency, equalTo(strong)))
      )
    },
    test("consistency sets ScanSome") {
      assert(DynamoDBQuery.scanSome(table1, 10).consistency(strong))(
        isSubtype[DynamoDBQuery.ScanSome](hasField("consistency", _.consistency, equalTo(strong)))
      )
    },
    test("consistency sets QuerySome") {
      assert(DynamoDBQuery.querySome(table1, 10).consistency(strong))(
        isSubtype[DynamoDBQuery.QuerySome](hasField("consistency", _.consistency, equalTo(strong)))
      )
    },
    test("consistency propagates through ZipPar to both branches") {
      val q = (GetItem(table1, pk) zipPar GetItem(table2, pk)).consistency(strong)
      q match {
        case DynamoDBQuery.ZipPar(l, r, _) =>
          assert(l)(isSubtype[GetItem](hasField("consistency", _.consistency, equalTo(strong)))) &&
          assert(r)(isSubtype[GetItem](hasField("consistency", _.consistency, equalTo(strong))))
        case _                             => assertTrue(false)
      }
    },
    test("consistency is a no-op for constructors without a consistency field") {
      val q = PutItem(table1, item1)
      assertTrue(q.consistency(strong) eq q)
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
      q match {
        case DynamoDBQuery.ZipPar(l, r, _) =>
          assert(l)(isSubtype[PutItem](hasField("returnValues", _.returnValues, equalTo(allOld)))) &&
          assert(r)(isSubtype[DeleteItem](hasField("returnValues", _.returnValues, equalTo(allOld))))
        case _                             => assertTrue(false)
      }
    },
    test("returns is a no-op for constructors without a returnValues field") {
      val q = GetItem(table1, pk)
      assertTrue(q.returns(allOld) eq q)
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
        DynamoDBQuery
          .batchWriteItem(List(item1))(i => PutItem(table1, i))
          .itemMetrics == ReturnItemCollectionMetrics.None
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
      assert(DynamoDBQuery.batchWriteItem(List(item1))(i => PutItem(table1, i)).metrics(sizeMetrics))(
        isSubtype[DynamoDBQuery.BatchWriteItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))
      )
    },
    test("metrics propagates through ZipPar") {
      val q = (PutItem(table1, item1) zipPar DeleteItem(table2, pk)).metrics(sizeMetrics)
      q match {
        case DynamoDBQuery.ZipPar(l, r, _) =>
          assert(l)(isSubtype[PutItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics)))) &&
          assert(r)(isSubtype[DeleteItem](hasField("itemMetrics", _.itemMetrics, equalTo(sizeMetrics))))
        case _                             => assertTrue(false)
      }
    },
    test("metrics is a no-op for constructors without an itemMetrics field") {
      val q = GetItem(table1, pk)
      assertTrue(q.metrics(sizeMetrics) eq q)
    }
  )

  private val someKey: Option[PrimaryKey] = Some(PrimaryKey("id" -> "a"))

  private val startKeySuite = suite("startKey")(
    test("ScanSome defaults to None") {
      assert(DynamoDBQuery.scanSome(table1, 10))(
        isSubtype[DynamoDBQuery.ScanSome](
          hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(None: Option[PrimaryKey]))
        )
      )
    },
    test("QuerySome defaults to None") {
      assert(DynamoDBQuery.querySome(table1, 10))(
        isSubtype[DynamoDBQuery.QuerySome](
          hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(None: Option[PrimaryKey]))
        )
      )
    },
    test("startKey sets ScanSome") {
      assert(DynamoDBQuery.scanSome(table1, 10).startKey(someKey))(
        isSubtype[DynamoDBQuery.ScanSome](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
      )
    },
    test("startKey sets QuerySome") {
      assert(DynamoDBQuery.querySome(table1, 10).startKey(someKey))(
        isSubtype[DynamoDBQuery.QuerySome](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
      )
    },
    test("startKey(None) resets exclusiveStartKey on ScanSome") {
      assert(DynamoDBQuery.scanSome(table1, 10).startKey(someKey).startKey(None))(
        isSubtype[DynamoDBQuery.ScanSome](
          hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(None: Option[PrimaryKey]))
        )
      )
    },
    test("startKey propagates through ZipPar to both branches") {
      val q = (DynamoDBQuery.scanSome(table1, 10) zipPar DynamoDBQuery.scanSome(table2, 10)).startKey(someKey)
      q match {
        case DynamoDBQuery.ZipPar(l, r, _) =>
          assert(l)(
            isSubtype[DynamoDBQuery.ScanSome](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
          ) &&
          assert(r)(
            isSubtype[DynamoDBQuery.ScanSome](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
          )
        case _                             => assertTrue(false)
      }
    },
    test("startKey propagates through Map") {
      val q = DynamoDBQuery.scanSome(table1, 10).map(identity).startKey(someKey)
      q match {
        case DynamoDBQuery.Map(inner, _) =>
          assert(inner)(
            isSubtype[DynamoDBQuery.ScanSome](hasField("exclusiveStartKey", _.exclusiveStartKey, equalTo(someKey)))
          )
        case _                           => assertTrue(false)
      }
    },
    test("startKey is a no-op for constructors without an exclusiveStartKey field") {
      val q = GetItem(table1, pk)
      assertTrue(q.startKey(someKey) eq q)
    }
  )

  private val selectSuite = suite("select")(
    test("selectCount sets Select.Count on ScanSome") {
      assert(DynamoDBQuery.scanSome(table1, 10).selectCount)(
        isSubtype[DynamoDBQuery.ScanSome](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
      )
    },
    test("selectCount sets Select.Count on QuerySome") {
      assert(DynamoDBQuery.querySome(table1, 10).selectCount)(
        isSubtype[DynamoDBQuery.QuerySome](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
      )
    },
    test("selectAllAttributes sets Select.AllAttributes on ScanSome") {
      assert(DynamoDBQuery.scanSome(table1, 10).selectAllAttributes)(
        isSubtype[DynamoDBQuery.ScanSome](
          hasField("select", _.select, equalTo(Some(Select.AllAttributes): Option[Select]))
        )
      )
    },
    test("selectAllProjectedAttributes sets Select.AllProjectedAttributes on QuerySome") {
      assert(DynamoDBQuery.querySome(table1, 10).selectAllProjectedAttributes)(
        isSubtype[DynamoDBQuery.QuerySome](
          hasField("select", _.select, equalTo(Some(Select.AllProjectedAttributes): Option[Select]))
        )
      )
    },
    test("selectSpecificAttributes sets Select.SpecificAttributes on ScanSome") {
      assert(DynamoDBQuery.scanSome(table1, 10).selectSpecificAttributes)(
        isSubtype[DynamoDBQuery.ScanSome](
          hasField("select", _.select, equalTo(Some(Select.SpecificAttributes): Option[Select]))
        )
      )
    },
    test("select propagates through ZipPar to both branches") {
      val q = (DynamoDBQuery.scanSome(table1, 10) zipPar DynamoDBQuery.scanSome(table2, 10)).selectCount
      q match {
        case DynamoDBQuery.ZipPar(l, r, _) =>
          assert(l)(
            isSubtype[DynamoDBQuery.ScanSome](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
          ) &&
          assert(r)(
            isSubtype[DynamoDBQuery.ScanSome](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
          )
        case _                             => assertTrue(false)
      }
    },
    test("select propagates through Map") {
      val q = DynamoDBQuery.scanSome(table1, 10).map(identity).selectCount
      q match {
        case DynamoDBQuery.Map(inner, _) =>
          assert(inner)(
            isSubtype[DynamoDBQuery.ScanSome](hasField("select", _.select, equalTo(Some(Select.Count): Option[Select])))
          )
        case _                           => assertTrue(false)
      }
    },
    test("select is a no-op for non-scan/query constructors") {
      val q = GetItem(table1, key1)
      assertTrue(q.selectCount eq q)
    }
  )

  private val createTableBase: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.createTable(
      table1,
      KeySchema("id"),
      NonEmptySet(AttributeDefinition.attrDefnString("id")),
      BillingMode.PayPerRequest
    )

  private val createTableComposite: DynamoDBQuery[Any, Unit] =
    DynamoDBQuery.createTable(
      table1,
      KeySchema("id", "year"),
      NonEmptySet(AttributeDefinition.attrDefnString("id"), AttributeDefinition.attrDefnString("year")),
      BillingMode.PayPerRequest
    )

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
    test("gsi is a no-op for non-CreateTable queries") {
      val q = GetItem(table1, key1)
      assertTrue(q.gsi("score-index", KeySchema("score"), ProjectionType.All) eq q)
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
    test("lsi is a no-op for non-CreateTable queries") {
      val q = GetItem(table1, key1)
      assertTrue(q.lsi("score-index", KeySchema("id", "score")) eq q)
    }
  )
}
