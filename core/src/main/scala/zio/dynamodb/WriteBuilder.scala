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

/**
 * A put / update / delete that executes to `Out` - `Unit` unless `.returning` asks for the item
 * DynamoDB sends back, which is then decoded to `Returned`. `.where` makes it a
 * [[ConditionalWriteBuilder]]. The general write modifiers (`capacity`, `metrics`,
 * `withRetryPolicy`) keep the builder, so they compose with `.where` / `.returning` in any order.
 */
abstract class WriteBuilder[In, Out, Returned] private[dynamodb] (
  private[dynamodb] val write: DynamoDBQuery[In, Option[Item]],
  private[dynamodb] val finish: Option[Item] => Out
) {

  /**
   * Makes the write conditional. A failed condition fails the effect with
   * [[DynamoDBError.ItemError.ConditionFailed]], whose `item` holds the item as it was -
   * `ReturnValuesOnConditionCheckFailure.AllOld` is sent by default (no read capacity is consumed;
   * only the failure response is larger). Turn it off with
   * [[ConditionalWriteBuilder.withoutItemOnConditionFailure]].
   */
  def where(ce: ConditionExpression[_]): ConditionalWriteBuilder[In, Out, Returned]

  /** Returns the item DynamoDB sends back: `None` when there was none. */
  def returning(rv: ReturnValues): WriteBuilder[In, Returned, Returned]

  def capacity(capacity: ReturnConsumedCapacity): WriteBuilder[In, Out, Returned]
  def metrics(itemMetrics: ReturnItemCollectionMetrics): WriteBuilder[In, Out, Returned]
  def withRetryPolicy(policy: RetryPolicy): WriteBuilder[In, Out, Returned]

  def toQuery: DynamoDBQuery[In, Out] = write.map(finish)

  def execute[F[_]](implicit interpreter: Interpreter[F]): F[Out] =
    interpreter.run(toQuery)
}

private[dynamodb] object WriteBuilder {
  val discard: Option[Item] => Unit = _ => ()

  def conditional[In](
    write: DynamoDBQuery[In, Option[Item]],
    ce: ConditionExpression[_]
  ): DynamoDBQuery[In, Option[Item]] =
    write.withCondition(ce).returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)

  def withoutItemOnConditionFailure[In](write: DynamoDBQuery[In, Option[Item]]): DynamoDBQuery[In, Option[Item]] =
    write.returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.None)
}

/**
 * A write with a condition. A failed condition fails the effect with
 * [[DynamoDBError.ItemError.ConditionFailed]], whose `item` is the item as it was (returned by
 * default - see [[withoutItemOnConditionFailure]]); every other failure is the underlying AWS SDK
 * exception.
 */
abstract class ConditionalWriteBuilder[In, Out, Returned] private[dynamodb] (
  private[dynamodb] val write: DynamoDBQuery[In, Option[Item]],
  private[dynamodb] val finish: Option[Item] => Out
) {

  /** Replaces the condition. */
  def where(ce: ConditionExpression[_]): ConditionalWriteBuilder[In, Out, Returned]

  /**
   * Stops a failed condition from returning the existing item (`ConditionFailed.item` is then
   * `None`): for very large items or frequently failing conditions, where the larger failure
   * response isn't wanted.
   */
  def withoutItemOnConditionFailure: ConditionalWriteBuilder[In, Out, Returned]

  def returning(rv: ReturnValues): ConditionalWriteBuilder[In, Returned, Returned]

  def capacity(capacity: ReturnConsumedCapacity): ConditionalWriteBuilder[In, Out, Returned]
  def metrics(itemMetrics: ReturnItemCollectionMetrics): ConditionalWriteBuilder[In, Out, Returned]
  def withRetryPolicy(policy: RetryPolicy): ConditionalWriteBuilder[In, Out, Returned]

  def toQuery: DynamoDBQuery[In, Out] = write.map(finish)

  def execute[F[_]](implicit interpreter: Interpreter[F]): F[Out] =
    interpreter.run(toQuery)
}

// The Low-Level implementations: the returned item stays a raw `Item`.
private[dynamodb] final class ItemWriteBuilder[In, Out](
  write0: DynamoDBQuery[In, Option[Item]],
  finish0: Option[Item] => Out
) extends WriteBuilder[In, Out, Option[Item]](write0, finish0) {
  def where(ce: ConditionExpression[_]): ConditionalWriteBuilder[In, Out, Option[Item]]      =
    new ConditionalItemWriteBuilder(WriteBuilder.conditional(write, ce), finish)
  def returning(rv: ReturnValues): WriteBuilder[In, Option[Item], Option[Item]]              =
    new ItemWriteBuilder(write.returns(rv), ItemWriteBuilder.raw)
  def capacity(capacity: ReturnConsumedCapacity): WriteBuilder[In, Out, Option[Item]]        =
    new ItemWriteBuilder(write.capacity(capacity), finish)
  def metrics(itemMetrics: ReturnItemCollectionMetrics): WriteBuilder[In, Out, Option[Item]] =
    new ItemWriteBuilder(write.metrics(itemMetrics), finish)
  def withRetryPolicy(policy: RetryPolicy): WriteBuilder[In, Out, Option[Item]]              =
    new ItemWriteBuilder(write.withRetryPolicy(policy), finish)
}

private[dynamodb] object ItemWriteBuilder {
  val raw: Option[Item] => Option[Item] = identity
}

private[dynamodb] final class ConditionalItemWriteBuilder[In, Out](
  write0: DynamoDBQuery[In, Option[Item]],
  finish0: Option[Item] => Out
) extends ConditionalWriteBuilder[In, Out, Option[Item]](write0, finish0) {
  def where(ce: ConditionExpression[_]): ConditionalWriteBuilder[In, Out, Option[Item]]                 =
    new ConditionalItemWriteBuilder(write.withCondition(ce), finish)
  def withoutItemOnConditionFailure: ConditionalWriteBuilder[In, Out, Option[Item]]                     =
    new ConditionalItemWriteBuilder(WriteBuilder.withoutItemOnConditionFailure(write), finish)
  def returning(rv: ReturnValues): ConditionalWriteBuilder[In, Option[Item], Option[Item]]              =
    new ConditionalItemWriteBuilder(write.returns(rv), ItemWriteBuilder.raw)
  def capacity(capacity: ReturnConsumedCapacity): ConditionalWriteBuilder[In, Out, Option[Item]]        =
    new ConditionalItemWriteBuilder(write.capacity(capacity), finish)
  def metrics(itemMetrics: ReturnItemCollectionMetrics): ConditionalWriteBuilder[In, Out, Option[Item]] =
    new ConditionalItemWriteBuilder(write.metrics(itemMetrics), finish)
  def withRetryPolicy(policy: RetryPolicy): ConditionalWriteBuilder[In, Out, Option[Item]]              =
    new ConditionalItemWriteBuilder(write.withRetryPolicy(policy), finish)
}
