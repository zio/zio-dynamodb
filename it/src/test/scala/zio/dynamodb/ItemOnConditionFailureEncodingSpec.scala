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
  ReturnValuesOnConditionCheckFailure => AwsReturnValuesOnConditionCheckFailure,
  _
}
import zio._
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.ddbexpr.DdbExprApi
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.test.Assertion._
import zio.test._

// No DynamoDB needed: a real ZioInterpreter over a stub client that captures the requests, so the
// assertions see exactly what would be sent - including the transaction path, which unwraps HL items.
object ItemOnConditionFailureEncodingSpec extends ZIOSpecDefault {

  final case class Score(id: String, score: Int)
  object Score extends CompanionOptics[Score] {
    implicit val schema: Schema[Score] = Schema.derived
    val id: Lens[Score, String]        = $(_.id)
    val score: Lens[Score, Int]        = $(_.score)
  }
  private val table = DdbExprApi.Table[Score]("scores")

  private final class Capturing extends AwsDynamoDB[Task] {
    @volatile var put: PutItemRequest                                                        = null
    @volatile var transactWrite: TransactWriteItemsRequest                                   = null
    def putItem(req: PutItemRequest): Task[PutItemResponse]                                  =
      ZIO.succeed { put = req; PutItemResponse.builder().build() }
    def transactWriteItems(req: TransactWriteItemsRequest): Task[TransactWriteItemsResponse] =
      ZIO.succeed { transactWrite = req; TransactWriteItemsResponse.builder().build() }
    def getItem(req: GetItemRequest): Task[GetItemResponse]                                  = ZIO.dieMessage("unused")
    def updateItem(req: UpdateItemRequest): Task[UpdateItemResponse]                         = ZIO.dieMessage("unused")
    def deleteItem(req: DeleteItemRequest): Task[DeleteItemResponse]                         = ZIO.dieMessage("unused")
    def batchGetItem(req: BatchGetItemRequest): Task[BatchGetItemResponse]                   = ZIO.dieMessage("unused")
    def batchWriteItem(req: BatchWriteItemRequest): Task[BatchWriteItemResponse]             = ZIO.dieMessage("unused")
    def query(req: QueryRequest): Task[QueryResponse]                                        = ZIO.dieMessage("unused")
    def scan(req: ScanRequest): Task[ScanResponse]                                           = ZIO.dieMessage("unused")
    def createTable(req: CreateTableRequest): Task[CreateTableResponse]                      = ZIO.dieMessage("unused")
    def deleteTable(req: DeleteTableRequest): Task[DeleteTableResponse]                      = ZIO.dieMessage("unused")
    def describeTable(req: DescribeTableRequest): Task[DescribeTableResponse]                = ZIO.dieMessage("unused")
    def transactGetItems(req: TransactGetItemsRequest): Task[TransactGetItemsResponse]       = ZIO.dieMessage("unused")
  }

  private val conditional = put(table, Score("alice", 99)).where(Score.score === 0)

  def spec = suite("ReturnValuesOnConditionCheckFailure as sent for HL conditional writes")(
    test("single put: ALL_OLD by default") {
      val client = new Capturing
      new ZioInterpreter(client)
        .run(conditional.toQuery)
        .as(
          assert(client.put.returnValuesOnConditionCheckFailure())(
            equalTo(AwsReturnValuesOnConditionCheckFailure.ALL_OLD)
          )
        )
    },
    test("single put: NONE with withoutItemOnConditionFailure") {
      val client = new Capturing
      new ZioInterpreter(client)
        .run(conditional.withoutItemOnConditionFailure.toQuery)
        .as(
          assert(client.put.returnValuesOnConditionCheckFailure())(equalTo(AwsReturnValuesOnConditionCheckFailure.NONE))
        )
    },
    test("transaction action: ALL_OLD by default") {
      val client = new Capturing
      new ZioInterpreter(client)
        .run(DynamoDBQuery.transactWriteItems(conditional.toQuery))
        .as(
          assert(client.transactWrite.transactItems().get(0).put().returnValuesOnConditionCheckFailure())(
            equalTo(AwsReturnValuesOnConditionCheckFailure.ALL_OLD)
          )
        )
    },
    test("transaction action: NONE with withoutItemOnConditionFailure") {
      val client = new Capturing
      new ZioInterpreter(client)
        .run(DynamoDBQuery.transactWriteItems(conditional.withoutItemOnConditionFailure.toQuery))
        .as(
          assert(client.transactWrite.transactItems().get(0).put().returnValuesOnConditionCheckFailure())(
            equalTo(AwsReturnValuesOnConditionCheckFailure.NONE)
          )
        )
    }
  )
}
