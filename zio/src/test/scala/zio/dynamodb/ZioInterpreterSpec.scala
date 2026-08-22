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

import software.amazon.awssdk.services.dynamodb.model._
import zio._
import zio.dynamodb.DynamoDBError.ItemError
import zio.test._
import zio.test.Assertion._

object ZioInterpreterSpec extends ZIOSpecDefault {

  // Stub that is never actually invoked — DynamoDBQuery.Fail / Absolve
  // short-circuit before any AWS operation is dispatched.
  private val stub: AwsDynamoDB[Task] = new AwsDynamoDB[Task] {
    private def notImplemented                                                               = ZIO.die(new NotImplementedError("stub"))
    def getItem(req: GetItemRequest): Task[GetItemResponse]                                  = notImplemented
    def putItem(req: PutItemRequest): Task[PutItemResponse]                                  = notImplemented
    def updateItem(req: UpdateItemRequest): Task[UpdateItemResponse]                         = notImplemented
    def deleteItem(req: DeleteItemRequest): Task[DeleteItemResponse]                         = notImplemented
    def batchGetItem(req: BatchGetItemRequest): Task[BatchGetItemResponse]                   = notImplemented
    def query(req: QueryRequest): Task[QueryResponse]                                        = notImplemented
    def scan(req: ScanRequest): Task[ScanResponse]                                           = notImplemented
    def createTable(req: CreateTableRequest): Task[CreateTableResponse]                      = notImplemented
    def deleteTable(req: DeleteTableRequest): Task[DeleteTableResponse]                      = notImplemented
    def batchWriteItem(req: BatchWriteItemRequest): Task[BatchWriteItemResponse]             = notImplemented
    def describeTable(req: DescribeTableRequest): Task[DescribeTableResponse]                = notImplemented
    def transactGetItems(req: TransactGetItemsRequest): Task[TransactGetItemsResponse]       = notImplemented
    def transactWriteItems(req: TransactWriteItemsRequest): Task[TransactWriteItemsResponse] = notImplemented
  }

  private val interp = new ZioInterpreter(stub)

  def spec = suite("ZioInterpreter error wrapping")(
    test("fail raises DynamoDBError directly, not wrapped in RuntimeException") {
      val error = ItemError.ValueNotFound("missing")
      for {
        exit <- interp.run(DynamoDBQuery.fail(error)).exit
      } yield assert(exit)(fails(isSubtype[DynamoDBError](anything)))
    },
    test("absolve on Left raises DynamoDBError directly, not wrapped in RuntimeException") {
      val error = ItemError.ValueNotFound("missing")
      val q     = DynamoDBQuery.Absolve(DynamoDBQuery(Left(error): Either[ItemError, Int]))
      for {
        exit <- interp.run(q).exit
      } yield assert(exit)(fails(isSubtype[DynamoDBError](anything)))
    }
  )
}
