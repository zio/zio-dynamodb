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

import scala.annotation.implicitNotFound

/**
 * Which operation a write builder holds, as a phantom type: `Put` and `Delete` can go in
 * `batchWriteItem` (DynamoDB batches have no update), `Update` can't.
 */
sealed trait WriteOp

object WriteOp {
  sealed trait Batchable extends WriteOp
  sealed trait Put       extends Batchable
  sealed trait Delete    extends Batchable
  sealed trait Update    extends WriteOp
}

/**
 * The `ReturnValues` a Low-Level write's `.returning` accepts, per operation - checked at compile time
 * on the value's own type, so pass a literal (`ReturnValues.AllOld`), not a value typed `ReturnValues`.
 */
@implicitNotFound(
  "${R} isn't valid here: putItem / deleteItem accept only ReturnValues.AllOld; updateItem accepts " +
    "ReturnValues.AllOld, AllNew, UpdatedOld or UpdatedNew. Pass the value itself (e.g. ReturnValues.AllOld), " +
    "not a value typed ReturnValues"
)
sealed trait ItemReturnValues[Op <: WriteOp, R <: ReturnValues]

object ItemReturnValues {
  implicit val putAllOld: ItemReturnValues[WriteOp.Put, ReturnValues.AllOld.type]               =
    new ItemReturnValues[WriteOp.Put, ReturnValues.AllOld.type] {}
  implicit val deleteAllOld: ItemReturnValues[WriteOp.Delete, ReturnValues.AllOld.type]         =
    new ItemReturnValues[WriteOp.Delete, ReturnValues.AllOld.type] {}
  implicit val updateAllOld: ItemReturnValues[WriteOp.Update, ReturnValues.AllOld.type]         =
    new ItemReturnValues[WriteOp.Update, ReturnValues.AllOld.type] {}
  implicit val updateAllNew: ItemReturnValues[WriteOp.Update, ReturnValues.AllNew.type]         =
    new ItemReturnValues[WriteOp.Update, ReturnValues.AllNew.type] {}
  implicit val updateUpdatedOld: ItemReturnValues[WriteOp.Update, ReturnValues.UpdatedOld.type] =
    new ItemReturnValues[WriteOp.Update, ReturnValues.UpdatedOld.type] {}
  implicit val updateUpdatedNew: ItemReturnValues[WriteOp.Update, ReturnValues.UpdatedNew.type] =
    new ItemReturnValues[WriteOp.Update, ReturnValues.UpdatedNew.type] {}
}

/**
 * The Low-Level write builder returned by `putItem` / `updateItem` / `deleteItem`: `.returning` gives
 * back the raw `Item`. A plain put or delete also goes in `batchWriteItem`.
 */
final class ItemWriteBuilder[In, Out, Op <: WriteOp] private[dynamodb] (
  write0: DynamoDBQuery[In, Option[Item]],
  finish0: Option[Item] => Out
) extends WriteBuilder[In, Out](write0, finish0) {
  def where(ce: ConditionExpression[In]): ConditionalItemWriteBuilder[In, Out, Op] =
    new ConditionalItemWriteBuilder(WriteBuilder.conditional(write, ce), finish)

  /**
   * Executes to the item DynamoDB returns (`None` when it returns none) instead of `Unit`; which
   * `ReturnValues` the operation accepts is checked at compile time - see [[ItemReturnValues]].
   */
  def returning[R <: ReturnValues](rv: R)(implicit
    valid: ItemReturnValues[Op, R]
  ): ItemWriteBuilder[In, Option[Item], Op]                                                     =
    new ItemWriteBuilder(write.returns(rv), ItemWriteBuilder.raw)
  override def capacity(capacity: ReturnConsumedCapacity): ItemWriteBuilder[In, Out, Op]        =
    new ItemWriteBuilder(write.capacity(capacity), finish)
  override def metrics(itemMetrics: ReturnItemCollectionMetrics): ItemWriteBuilder[In, Out, Op] =
    new ItemWriteBuilder(write.metrics(itemMetrics), finish)
  override def withRetryPolicy(policy: RetryPolicy): ItemWriteBuilder[In, Out, Op]              =
    new ItemWriteBuilder(write.withRetryPolicy(policy), finish)
}

private[dynamodb] object ItemWriteBuilder {
  val raw: Option[Item] => Option[Item] = identity
}

/** [[ItemWriteBuilder]] with a condition - see [[ConditionalWriteBuilder]]. */
final class ConditionalItemWriteBuilder[In, Out, Op <: WriteOp] private[dynamodb] (
  write0: DynamoDBQuery[In, Option[Item]],
  finish0: Option[Item] => Out
) extends ConditionalWriteBuilder[In, Out](write0, finish0) {
  def where(ce: ConditionExpression[In]): ConditionalItemWriteBuilder[In, Out, Op] =
    new ConditionalItemWriteBuilder(write.withCondition(ce), finish)
  def withoutItemOnConditionFailure: ConditionalItemWriteBuilder[In, Out, Op]      =
    new ConditionalItemWriteBuilder(WriteBuilder.withoutItemOnConditionFailure(write), finish)

  /** See [[ItemWriteBuilder.returning]]. */
  def returning[R <: ReturnValues](rv: R)(implicit
    valid: ItemReturnValues[Op, R]
  ): ConditionalItemWriteBuilder[In, Option[Item], Op]                                                     =
    new ConditionalItemWriteBuilder(write.returns(rv), ItemWriteBuilder.raw)
  override def capacity(capacity: ReturnConsumedCapacity): ConditionalItemWriteBuilder[In, Out, Op]        =
    new ConditionalItemWriteBuilder(write.capacity(capacity), finish)
  override def metrics(itemMetrics: ReturnItemCollectionMetrics): ConditionalItemWriteBuilder[In, Out, Op] =
    new ConditionalItemWriteBuilder(write.metrics(itemMetrics), finish)
  override def withRetryPolicy(policy: RetryPolicy): ConditionalItemWriteBuilder[In, Out, Op]              =
    new ConditionalItemWriteBuilder(write.withRetryPolicy(policy), finish)
}
