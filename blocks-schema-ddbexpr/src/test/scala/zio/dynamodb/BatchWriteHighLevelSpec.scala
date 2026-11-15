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
import zio.blocks.schema.{ CompanionOptics, Lens, Modifier, Schema }
import zio.dynamodb.DynamoDBQuery.BatchWriteItem
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.dynamodb.blocks.schema.ddbexpr.DdbExprApi
import zio.dynamodb.blocks.schema.ddbexpr.DdbExprApi._
import zio.dynamodb.blocks.schema.ddbexpr.DdbKeyExpr._
import zio.dynamodb.blocks.schema.ddbexpr.DdbExpr._
import zio.test._

/**
 * High-Level `put` / `deleteFrom` as the items of a Low-Level `batchWriteItem`: each write goes
 * through the table's codec (so a renamed key field is respected), Low-Level `putItem` /
 * `deleteItem` still work alongside them, and only plain puts and deletes compile - the batch has
 * no conditions, ReturnValues or updates. The batch itself (`Batch.WriteResult`) stays Low-Level.
 */
object BatchWriteHighLevelSpec extends ZIOSpecDefault {

  final case class Customer(id: String, name: String)
  object Customer extends CompanionOptics[Customer] {
    implicit val schema: Schema[Customer] = Schema.derived
    val id: Lens[Customer, String]        = $(_.id)
    val name: Lens[Customer, String]      = $(_.name)
  }

  // The key field is stored as "customer_id" - a hand-written PrimaryKey("customerId" -> ...) would miss it.
  private val customers =
    DdbExprApi.Table(
      "customers",
      Customer.schema.deriving(DynamoDBCodecDeriver).modifier(Customer.id, Modifier.rename("customer_id")).derive
    )

  private val ada   = Customer("c1", "Ada")
  private val grace = Customer("c2", "Grace")

  private def writes(q: DynamoDBQuery[Any, Batch.WriteResult]): Chunk[BatchWriteItem.Write] =
    q.asInstanceOf[BatchWriteItem].addList

  private def itemError(q: DynamoDBQuery[Any, Batch.WriteResult]): Option[DynamoDBError] =
    q.asInstanceOf[BatchWriteItem].itemError

  def spec = suite("batchWriteItem with High-Level writes")(
    test("put encodes each value with the table's codec") {
      val q = DynamoDBQuery.batchWriteItem(List(ada, grace))(c => put(customers, c))
      assertTrue(
        writes(q) == Chunk(
          BatchWriteItem.Put(Item("customer_id" -> "c1", "name" -> "Ada")),
          BatchWriteItem.Put(Item("customer_id" -> "c2", "name" -> "Grace"))
        ),
        itemError(q).isEmpty
      )
    },
    test("deleteFrom resolves the key through the table's codec, including the renamed field") {
      val q =
        DynamoDBQuery.batchWriteItem(List("c1", "c2"))(id => deleteFrom(customers)(Customer.id.partitionKey === id))
      assertTrue(
        writes(q) == Chunk(
          BatchWriteItem.Delete(PrimaryKey("customer_id" -> "c1")),
          BatchWriteItem.Delete(PrimaryKey("customer_id" -> "c2"))
        ),
        itemError(q).isEmpty
      )
    },
    test("Low-Level putItem / deleteItem still work, and mix with High-Level writes") {
      val q = DynamoDBQuery.batchWriteItem(List(1, 2, 3, 4)) {
        case 1 => put(customers, ada)
        case 2 => deleteFrom(customers)(Customer.id.partitionKey === "c2")
        case 3 => DynamoDBQuery.putItem("customers", Item("customer_id" -> "c3", "name" -> "Hopper"))
        case _ => DynamoDBQuery.deleteItem("customers", PrimaryKey("customer_id" -> "c4"))
      }
      assertTrue(
        writes(q) == Chunk(
          BatchWriteItem.Put(Item("customer_id" -> "c1", "name" -> "Ada")),
          BatchWriteItem.Delete(PrimaryKey("customer_id" -> "c2")),
          BatchWriteItem.Put(Item("customer_id" -> "c3", "name" -> "Hopper")),
          BatchWriteItem.Delete(PrimaryKey("customer_id" -> "c4"))
        ),
        itemError(q).isEmpty
      )
    },
    test("a value that doesn't encode becomes the batch's error rather than throwing") {
      // A hand-written top-level codec that doesn't produce a record can't be put as an item.
      val notARecord = DdbExprApi.Table(
        "customers",
        new DynamoDBCodec[Customer] {
          def encoder: Encoder[Customer] = c => AttributeValue.String(c.id)
          def decoder: Decoder[Customer] = _ => Left(DynamoDBError.ItemError.DecodingError("unused"))
        }
      )
      val q          = DynamoDBQuery.batchWriteItem(List(ada))(c => put(notARecord, c))
      assertTrue(writes(q).isEmpty, itemError(q).isDefined)
    },
    test("only plain puts and deletes compile: no condition, ReturnValues or update") {
      for {
        plain          <- typeCheck("DynamoDBQuery.batchWriteItem(List(ada))(c => put(customers, c))")
        where          <- typeCheck(
                            """DynamoDBQuery.batchWriteItem(List(ada))(c => put(customers, c).where(Customer.name === "Ada"))"""
                          )
        returning      <- typeCheck(
                            "DynamoDBQuery.batchWriteItem(List(ada))(c => put(customers, c).returning(ReturnValues.AllOld))"
                          )
        upd            <-
          typeCheck(
            """DynamoDBQuery.batchWriteItem(List(ada))(c => update(customers)(Customer.id.partitionKey === c.id)(Customer.name.set("x")))"""
          )
        // controls: each rejected write compiles on its own, so the batch is what rejects it
        whereAlone     <- typeCheck("""put(customers, ada).where(Customer.name === "Ada")""")
        returningAlone <- typeCheck("put(customers, ada).returning(ReturnValues.AllOld)")
        updAlone       <- typeCheck("""update(customers)(Customer.id.partitionKey === "c1")(Customer.name.set("x"))""")
      } yield assertTrue(
        plain.isRight,
        where.isLeft,
        returning.isLeft,
        upd.isLeft,
        whereAlone.isRight,
        returningAlone.isRight,
        updAlone.isRight
      )
    }
  )
}
