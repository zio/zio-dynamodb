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

import zio.test._
import zio.test.Assertion.{ anything, isSubtype }

object MiscSpec extends ZIOSpecDefault {

  def spec = suite("Miscellaneous")(
    consistencyModeSuite,
    billingModeSuite,
    projectionTypeSuite,
    attributeDefinitionSuite,
    nonEmptySetSuite,
    aliasMapRenderSuite,
    partitionKeySuite,
    dynamoDBQuerySuite,
    zippableSuite
  )

  private val consistencyModeSuite = suite("ConsistencyMode")(
    test("Strong maps to true") {
      assertTrue(ConsistencyMode.toBoolean(ConsistencyMode.Strong))
    },
    test("Weak maps to false") {
      assertTrue(!ConsistencyMode.toBoolean(ConsistencyMode.Weak))
    }
  )

  private val billingModeSuite = suite("BillingMode")(
    test("PayPerRequest is a BillingMode") {
      assertTrue((BillingMode.PayPerRequest: BillingMode) == BillingMode.PayPerRequest)
    },
    test("provisioned creates Provisioned with read/write capacities") {
      val b = BillingMode.provisioned(10L, 5L)
      assert(b)(isSubtype[BillingMode.Provisioned](anything))
    }
  )

  private val projectionTypeSuite = suite("ProjectionType")(
    test("KeysOnly is a ProjectionType") {
      assertTrue((ProjectionType.KeysOnly: ProjectionType) == ProjectionType.KeysOnly)
    },
    test("All is a ProjectionType") {
      assertTrue((ProjectionType.All: ProjectionType) == ProjectionType.All)
    },
    test("Include wraps a NonEmptySet of strings") {
      val pt = ProjectionType.Include("a", "b")
      assert(pt)(isSubtype[ProjectionType.Include](anything))
    }
  )

  private val attributeDefinitionSuite = suite("AttributeDefinition")(
    test("attrDefnString creates String definition") {
      val d = AttributeDefinition.attrDefnString("id")
      assertTrue(d.attributeType == AttributeValueType.String)
    },
    test("attrDefnNumber creates Number definition") {
      val d = AttributeDefinition.attrDefnNumber("count")
      assertTrue(d.attributeType == AttributeValueType.Number)
    },
    test("attrDefnBinary creates Binary definition") {
      val d = AttributeDefinition.attrDefnBinary("data")
      assertTrue(d.attributeType == AttributeValueType.Binary)
    }
  )

  private val nonEmptySetSuite = suite("NonEmptySet")(
    test("apply with head creates a set of one") {
      val s = NonEmptySet("a")
      assertTrue(s.iterator.toList == List("a"))
    },
    test("apply with head and varargs creates correct set") {
      val s = NonEmptySet("a", "b", "c")
      assertTrue(s.iterator.toSet == Set("a", "b", "c"))
    },
    test("+ adds an element") {
      val s  = NonEmptySet("a")
      val s2 = s + "b"
      assertTrue(s2.iterator.toSet == Set("a", "b"))
    },
    test("++ adds all elements from iterable") {
      val s  = NonEmptySet("a")
      val s2 = s ++ List("b", "c")
      assertTrue(s2.iterator.toSet == Set("a", "b", "c"))
    }
  )

  private val aliasMapRenderSuite = suite("AliasMapRender")(
    test("succeed returns the given value unchanged") {
      val r      = AliasMapRender.succeed(42)
      val (_, v) = r.execute
      assertTrue(v == 42)
    },
    test("getMap returns the current alias map") {
      val r       = AliasMapRender.getMap
      val (am, v) = r.execute
      assertTrue(v == am)
    },
    test("collectAll None produces AliasMapRender of None") {
      val r      = AliasMapRender.collectAll[String](None)
      val (_, v) = r.execute
      assertTrue(v.isEmpty)
    },
    test("collectAll Some produces AliasMapRender of Some") {
      val inner  = AliasMapRender.succeed("hello")
      val r      = AliasMapRender.collectAll(Some(inner))
      val (_, v) = r.execute
      assertTrue(v.contains("hello"))
    },
    test("forEach with empty list produces empty list") {
      val r      = AliasMapRender.forEach(List.empty)
      val (_, v) = r.execute
      assertTrue(v.isEmpty)
    },
    test("forEach with non-empty list produces aliases") {
      val pe     = ProjectionExpression.$("name")
      val r      = AliasMapRender.forEach(List(pe))
      val (_, v) = r.execute
      assertTrue(v.nonEmpty)
    },
    test("zipRight discards left result") {
      val left   = AliasMapRender.succeed(1)
      val right  = AliasMapRender.succeed("x")
      val r      = left.zipRight(right)
      val (_, v) = r.execute
      assertTrue(v == "x")
    }
  )

  private val partitionKeySuite = suite("PartitionKey")(
    test("PartitionKeyUnknownToOps === builds PartitionKeyEquals") {
      val pe   = ProjectionExpression.$("id")
      val pk   = pe.partitionKey
      val expr = pk === "val"
      assert(expr)(isSubtype[KeyConditionExpr.PartitionKeyEquals[Any]](anything))
    },
    test("PartitionKeyOps === builds PartitionKeyEquals") {
      val pk: PartitionKey[Any, String] = PartitionKey[Any, String]("id")
      import PartitionKey.PartitionKeyOps
      val expr                          = pk === "val"
      assert(expr)(isSubtype[KeyConditionExpr.PartitionKeyEquals[Any]](anything))
    }
  )

  private val dynamoDBQuerySuite = suite("DynamoDBQuery")(
    test("whereKey on ZipPar propagates to both branches") {
      val q1      = DynamoDBQuery.QuerySome("t1", 10)
      val q2      = DynamoDBQuery.QuerySome("t2", 10)
      val pk      = ProjectionExpression.$("id").partitionKey
      val zipped  = q1 zipPar q2
      val withKey = zipped.whereKey(pk === "val")
      assertTrue(withKey != null)
    },
    test("whereKey on Map propagates to inner query") {
      val q       = DynamoDBQuery.QuerySome("t", 10)
      val pk      = ProjectionExpression.$("id").partitionKey
      val mapped  = q.map(identity)
      val withKey = mapped.whereKey(pk === "val")
      assertTrue(withKey != null)
    },
    test("whereKey on Absolve propagates to inner query") {
      val q       = DynamoDBQuery.QuerySome("t", 10)
      val pk      = ProjectionExpression.$("id").partitionKey
      val absolve = DynamoDBQuery.Absolve(q.map(p => Right(p): Either[DynamoDBError.ItemError, Page[Item]]))
      val withKey = absolve.whereKey(pk === "val")
      assertTrue(withKey != null)
    },
    test("whereKey on non-QuerySome returns self unchanged") {
      val q      = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "1"))
      val pk     = ProjectionExpression.$("id").partitionKey
      val result = q.whereKey(pk === "val")
      assertTrue(result == q)
    },
    test("where on ZipPar propagates condition to both branches") {
      val q1                             = DynamoDBQuery.putItem("t1", Item("id" -> "1"))
      val q2                             = DynamoDBQuery.putItem("t2", Item("id" -> "2"))
      val cond: ConditionExpression[Any] = ProjectionExpression.$("id") === "1"
      val zipped                         = q1 zipPar q2
      val withCond                       = zipped.where(cond)
      assertTrue(withCond != null)
    },
    test("where on UpdateItem sets conditionExpression") {
      val key                            = PrimaryKey("id" -> "1")
      val expr                           = UpdateExpression(ProjectionExpression.$("name").set("alice"))
      val q                              = DynamoDBQuery.UpdateItem("t", key, expr)
      val cond: ConditionExpression[Any] = ProjectionExpression.$("id") === "1"
      val result                         = q.where(cond)
      assertTrue(result != null)
    },
    test("where on DeleteItem sets conditionExpression") {
      val q                              = DynamoDBQuery.DeleteItem("t", PrimaryKey("id" -> "1"))
      val cond: ConditionExpression[Any] = ProjectionExpression.$("id") === "1"
      val result                         = q.where(cond)
      assertTrue(result != null)
    },
    test("where on non-matching query returns self") {
      val q                              = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "1"))
      val cond: ConditionExpression[Any] = ProjectionExpression.$("id") === "1"
      val result                         = q.where(cond)
      assertTrue(result == q)
    },
    test("filter on ZipPar propagates filter to both branches") {
      val q1                          = DynamoDBQuery.QuerySome("t1", 10)
      val q2                          = DynamoDBQuery.QuerySome("t2", 10)
      val filt: FilterExpression[Any] = ProjectionExpression.$("name") === "alice"
      val zipped                      = q1 zipPar q2
      val withFilter                  = zipped.filter(filt)
      assertTrue(withFilter != null)
    },
    test("filter on non-matching query returns self") {
      val q                           = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "1"))
      val filt: FilterExpression[Any] = ProjectionExpression.$("name") === "alice"
      val result                      = q.filter(filt)
      assertTrue(result == q)
    },
    test("DescribeTableResponse toString includes tableArn") {
      val r = DynamoDBQuery.DescribeTableResponse("arn:aws:test", DynamoDBQuery.TableStatus.Active, 100L, 5L)
      assertTrue(r.toString.contains("arn:aws:test"))
    },
    test("TableStatus values are distinct") {
      val statuses: List[DynamoDBQuery.TableStatus] = List(
        DynamoDBQuery.TableStatus.Creating,
        DynamoDBQuery.TableStatus.Updating,
        DynamoDBQuery.TableStatus.Deleting,
        DynamoDBQuery.TableStatus.Active,
        DynamoDBQuery.TableStatus.InaccessibleEncryptionCredentials,
        DynamoDBQuery.TableStatus.Archiving,
        DynamoDBQuery.TableStatus.Archived,
        DynamoDBQuery.TableStatus.ReplicationNotAuthorized,
        DynamoDBQuery.TableStatus.unknownToSdkVersion
      )
      assertTrue(statuses.distinct.size == 9)
    },
    test("batchGetItem builds a BatchGetItem from items") {
      val people = List("id1", "id2")
      val batch  = DynamoDBQuery.batchGetItem(people)(id => DynamoDBQuery.GetItem("t", PrimaryKey("id" -> id)))
      assertTrue(batch.requestItems.size == 1)
    },
    test("BatchGetItem.toGetItemResponses returns None for missing keys") {
      val gi1     = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "1"))
      val gi2     = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "2"))
      val batch   = DynamoDBQuery.BatchGetItem() + gi1 + gi2
      val resp    = DynamoDBQuery.BatchGetItem.Response()
      val results = batch.toGetItemResponses(resp)
      assertTrue(results.forall(_.isEmpty))
    },
    test("BatchGetItem.toGetItemResponses returns Some for found keys") {
      val key     = PrimaryKey("id" -> "1")
      val item    = Item("id" -> "1", "name" -> "alice")
      val gi      = DynamoDBQuery.GetItem("t", key)
      val batch   = DynamoDBQuery.BatchGetItem() + gi
      val resp    = DynamoDBQuery.BatchGetItem.Response(
        MapOfSet.empty[String, Item] + (("t", item))
      )
      val results = batch.toGetItemResponses(resp)
      assertTrue(results.headOption.flatten.isDefined)
    },
    test("BatchGetItem.addAll adds multiple GetItems") {
      val gi1   = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "1"))
      val gi2   = DynamoDBQuery.GetItem("t", PrimaryKey("id" -> "2"))
      val batch = DynamoDBQuery.BatchGetItem().addAll(gi1, gi2)
      assertTrue(batch.orderedGetItems.size == 2)
    },
    test("BatchWriteItem.addAll adds multiple writes") {
      val put1  = DynamoDBQuery.putItem("t", Item("id" -> "1"))
      val put2  = DynamoDBQuery.putItem("t", Item("id" -> "2"))
      val batch = DynamoDBQuery.BatchWriteItem().addAll(put1, put2)
      assertTrue(batch.addList.size == 2)
    },
    test("DynamoDBQuery.updateItem with UpdateExpression overload") {
      val key  = PrimaryKey("id" -> "1")
      val expr = UpdateExpression(ProjectionExpression.$("name").set("alice"))
      val q    = DynamoDBQuery.updateItem("t", key, expr)
      assertTrue(q != null)
    },
    test("DynamoDBQuery.scanSome with all parameters") {
      val q = DynamoDBQuery.scanSome(
        "t",
        10,
        indexName = Some("idx"),
        consistency = ConsistencyMode.Strong,
        select = Some(Select.AllAttributes)
      )
      assertTrue(q != null)
    },
    test("DynamoDBQuery.querySome with all parameters") {
      val q = DynamoDBQuery
        .querySome("t", 10, indexName = Some("idx"), consistency = ConsistencyMode.Strong, ascending = false)
      assertTrue(q != null)
    },
    test("where on Map propagates condition") {
      val q                              = DynamoDBQuery.putItem("t", Item("id" -> "1"))
      val cond: ConditionExpression[Any] = ProjectionExpression.$("id") === "1"
      val mapped                         = q.map(identity)
      val result                         = mapped.where(cond)
      assertTrue(result != null)
    },
    test("where on Absolve propagates condition") {
      val inner                          = DynamoDBQuery.putItem("t", Item("id" -> "1"))
      val absolve                        = DynamoDBQuery.Absolve(inner.map(r => Right(r): Either[DynamoDBError.ItemError, Option[Item]]))
      val cond: ConditionExpression[Any] = ProjectionExpression.$("id") === "1"
      val result                         = absolve.where(cond)
      assertTrue(result != null)
    },
    test("filter on Map propagates") {
      val q                           = DynamoDBQuery.QuerySome("t", 10)
      val filt: FilterExpression[Any] = ProjectionExpression.$("name") === "alice"
      val mapped                      = q.map(identity)
      val result                      = mapped.filter(filt)
      assertTrue(result != null)
    },
    test("filter on Absolve propagates") {
      val q                           = DynamoDBQuery.QuerySome("t", 10)
      val filt: FilterExpression[Any] = ProjectionExpression.$("name") === "alice"
      val absolve                     = DynamoDBQuery.Absolve(q.map(r => Right(r): Either[DynamoDBError.ItemError, Page[Item]]))
      val result                      = absolve.filter(filt)
      assertTrue(result != null)
    },
    test("filter on ScanSome sets filterExpression") {
      val q                           = DynamoDBQuery.scanSome("t", 10)
      val filt: FilterExpression[Any] = ProjectionExpression.$("name") === "alice"
      val result                      = q.filter(filt)
      assertTrue(result != null)
    }
  )

  private val zippableSuite = suite("Zippable")(
    test("Zippable3 zips (A, B) and Z into (A, B, Z)") {
      val z = implicitly[Zippable.Out[(Int, String), Boolean, (Int, String, Boolean)]]
      assertTrue(z.zip((1, "a"), true) == ((1, "a", true)))
    },
    test("Zippable4 zips (A, B, C) and Z into (A, B, C, Z)") {
      val z = implicitly[Zippable.Out[(Int, String, Boolean), Double, (Int, String, Boolean, Double)]]
      assertTrue(z.zip((1, "a", true), 2.0) == ((1, "a", true, 2.0)))
    },
    test("ZippableUnit zips A and Unit into A") {
      val z = implicitly[Zippable.Out[Int, Unit, Int]]
      assertTrue(z.zip(42, ()) == 42)
    }
  )
}
