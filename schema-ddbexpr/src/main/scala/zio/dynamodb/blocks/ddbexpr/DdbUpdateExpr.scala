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

package zio.dynamodb.blocks.ddbexpr

import zio.blocks.schema.{ Optic, Schema }

/**
 * Deferred, typed update-expression ADT — the update-action counterpart of [[DdbExpr]].
 *
 *  Built by the [[DdbExprSyntax.OpticUpdateOps]] extension methods on
 *  [[zio.blocks.schema.Optic]] (`Task.score.set(1)`, `Task.tags.appendList(List("x"))`, …)
 *  and composed with `+`. Each value-carrying node holds a [[Schema]] for its operand rather
 *  than a pre-derived codec, so [[DdbUpdateExprInterpreter]] can encode literals — and resolve
 *  attribute paths — against the originating [[Table]]'s deriver configuration at
 *  `DdbExprApi.update` time, exactly as `.where` / `.filter` do for conditions. Building an
 *  action never touches implicit codec resolution.
 *
 *  Path-resolution failures for optic shapes DynamoDB paths can't represent are converted to
 *  core `UpdateExpression.Action.Failure` by [[DdbUpdateExprInterpreter]] and surface at query
 *  execution, alongside `ConditionExpression.Failure`.
 */
sealed trait DdbUpdateExpr[From] { self =>
  def +(that: DdbUpdateExpr[From]): DdbUpdateExpr[From] = DdbUpdateExpr.Combine(self, that)
}

object DdbUpdateExpr {

  /** SET path = value */
  final case class SetValue[From, A](optic: Optic[From, A], value: A, schema: Schema[A]) extends DdbUpdateExpr[From]

  /** SET path = other_path */
  final case class SetPath[From, A](optic: Optic[From, A], other: Optic[From, A]) extends DdbUpdateExpr[From]

  /** SET path = if_not_exists(path, value) */
  final case class SetIfNotExists[From, A](optic: Optic[From, A], value: A, schema: Schema[A])
      extends DdbUpdateExpr[From]

  /** REMOVE path */
  final case class Remove[From](optic: Optic[From, _]) extends DdbUpdateExpr[From]

  /** REMOVE path[index] */
  final case class RemoveAt[From](optic: Optic[From, _], index: Int) extends DdbUpdateExpr[From]

  /** SET path = path + delta */
  final case class Increment[From, A](optic: Optic[From, A], delta: A, schema: Schema[A]) extends DdbUpdateExpr[From]

  /** SET path = path - delta */
  final case class Decrement[From, A](optic: Optic[From, A], delta: A, schema: Schema[A]) extends DdbUpdateExpr[From]

  /** ADD path value (numeric) */
  final case class Add[From, A](optic: Optic[From, A], value: A, schema: Schema[A]) extends DdbUpdateExpr[From]

  /** ADD path set (NS/SS/BS union) */
  final case class AddToSet[From, A](optic: Optic[From, A], value: A, schema: Schema[A]) extends DdbUpdateExpr[From]

  /** DELETE path set (NS/SS/BS element removal) */
  final case class DeleteFromSet[From, A](optic: Optic[From, A], value: A, schema: Schema[A])
      extends DdbUpdateExpr[From]

  /** SET path = list_append(path, [items]) */
  final case class AppendList[From, B](optic: Optic[From, _], items: Seq[B], elemSchema: Schema[B])
      extends DdbUpdateExpr[From]

  /** SET path = list_append([items], path) */
  final case class PrependList[From, B](optic: Optic[From, _], items: Seq[B], elemSchema: Schema[B])
      extends DdbUpdateExpr[From]

  /** Two actions combined (`a + b`). */
  final case class Combine[From](left: DdbUpdateExpr[From], right: DdbUpdateExpr[From]) extends DdbUpdateExpr[From]
}
