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
import zio.dynamodb.ConditionExpression.Operand
import zio.dynamodb.KeyConditionExpr.ExtendedSortKeyExpr
import zio.dynamodb.UpdateExpression.{ Action, SetOperand }

// Gathers the errors from invalid `$` paths and `.partitionKey` / `.sortKey` keys anywhere in a
// leaf query, so the interpreter fails it with `InvalidPath` before building a request.
private[dynamodb] object InvalidPaths {

  def apply(query: DynamoDBQuery[_, _]): Option[DynamoDBError.QueryBuilderError.InvalidPath] =
    errors(query) match {
      case Nil    => None
      case errors => Some(DynamoDBError.QueryBuilderError.InvalidPath(errors.distinct.mkString("; ")))
    }

  private def errors(query: DynamoDBQuery[_, _]): List[String] =
    query match {
      case q: DynamoDBQuery.GetItem            => projections(q.projections)
      case q: DynamoDBQuery.PutItem            => condition(q.conditionExpression)
      case q: DynamoDBQuery.UpdateItem         => action(q.updateExpression.action) ++ condition(q.conditionExpression)
      case q: DynamoDBQuery.DeleteItem         => condition(q.conditionExpression)
      case q: DynamoDBQuery.ConditionCheck     => condition(q.conditionExpression)
      case q: DynamoDBQuery.Query              =>
        key(q.keyConditionExpr) ++ condition(q.filterExpression) ++ projections(q.projections)
      case q: DynamoDBQuery.Scan               => condition(q.filterExpression) ++ projections(q.projections)
      case q: DynamoDBQuery.BatchGetItem       =>
        q.requestItems.valuesIterator.flatMap(t => projections(t.projectionExpressionSet)).toList
      case q: DynamoDBQuery.TransactGetItems   => each(q.getItems)(getItemErrors)
      case q: DynamoDBQuery.TransactWriteItems => each(q.writeItems)(writeItemErrors)
      case _                                   => Nil
    }

  // Transaction write items can be wrapped, e.g. a High-Level put; check the leaf write inside.
  @scala.annotation.tailrec
  private def writeItem(q: DynamoDBQuery[_, _]): List[String] =
    q match {
      case DynamoDBQuery.Map(inner, _)  => writeItem(inner)
      case w: WriteNode[_, _]           => writeItem(w.write)
      case c: ConditionCheckBuilder[_]  => writeItem(c.check)
      case DynamoDBQuery.Absolve(inner) => writeItem(inner)
      case leaf                         => errors(leaf)
    }

  // Held in vals so passing them to `each` doesn't allocate a closure per call.
  private val getItemErrors: DynamoDBQuery.GetItem => List[String]           = g => projections(g.projections)
  private val writeItemErrors: DynamoDBQuery[Any, Any] => List[String]       = writeItem
  private val actionErrors: UpdateExpression.Action[Nothing] => List[String] = a => action(a)

  // An index loop rather than toList.flatMap, so a query whose paths are all valid allocates nothing here.
  private def each[A](as: Chunk[A])(f: A => List[String]): List[String] = {
    var errors: List[String] = Nil
    var i                    = 0
    while (i < as.length) {
      val e = f(as(i))
      if (e.nonEmpty) errors = errors ++ e
      i += 1
    }
    errors
  }

  private def path(pe: ProjectionExpression[_, _]): List[String] = ProjectionExpression.failureOf(pe).toList

  private def projections(pes: Iterable[ProjectionExpression[_, _]]): List[String] =
    if (pes.isEmpty) Nil else pes.toList.flatMap(path)

  private def condition(ce: Option[ConditionExpression[_]]): List[String] = ce.fold(List.empty[String])(condition)

  private def condition(ce: ConditionExpression[_]): List[String] =
    ce match {
      case c: ConditionExpression.Between[_]            => operand(c.left)
      case c: ConditionExpression.In[_]                 => operand(c.left)
      case c: ConditionExpression.AttributeExists[_]    => path(c.path)
      case c: ConditionExpression.AttributeNotExists[_] => path(c.path)
      case c: ConditionExpression.AttributeType[_]      => path(c.path)
      case c: ConditionExpression.Contains[_]           => path(c.path)
      case c: ConditionExpression.BeginsWith[_]         => path(c.path)
      case c: ConditionExpression.And[_]                => condition(c.left) ++ condition(c.right)
      case c: ConditionExpression.Or[_]                 => condition(c.left) ++ condition(c.right)
      case c: ConditionExpression.Not[_]                => condition(c.exprn)
      case c: ConditionExpression.Equals[_]             => operand(c.left) ++ operand(c.right)
      case c: ConditionExpression.NotEqual[_]           => operand(c.left) ++ operand(c.right)
      case c: ConditionExpression.LessThan[_]           => operand(c.left) ++ operand(c.right)
      case c: ConditionExpression.GreaterThan[_]        => operand(c.left) ++ operand(c.right)
      case c: ConditionExpression.LessThanOrEqual[_]    => operand(c.left) ++ operand(c.right)
      case c: ConditionExpression.GreaterThanOrEqual[_] => operand(c.left) ++ operand(c.right)
      case _: ConditionExpression.Failure[_]            => Nil
    }

  private def operand(op: Operand[_, _]): List[String] =
    op match {
      case o: Operand.ProjectionExpressionOperand[_] => path(o.pe)
      case o: Operand.Size[_, _]                     => path(o.path)
      case _: Operand.ValueOperand[_]                => Nil
    }

  private def action(a: Action[_]): List[String] =
    a match {
      case Action.Actions(actions)   => each(actions)(actionErrors)
      case Action.SetAction(p, op)   => path(p) ++ setOperand(op)
      case Action.RemoveAction(p)    => path(p)
      case Action.AddAction(p, _)    => path(p)
      case Action.DeleteAction(p, _) => path(p)
      case Action.Failure(_)         => Nil
    }

  private def setOperand(op: SetOperand[_]): List[String] =
    op match {
      case SetOperand.Minus(l, r)       => setOperand(l) ++ setOperand(r)
      case SetOperand.Plus(l, r)        => setOperand(l) ++ setOperand(r)
      case SetOperand.ValueOperand(_)   => Nil
      case SetOperand.PathOperand(p)    => path(p)
      case SetOperand.ListAppend(p, _)  => path(p)
      case SetOperand.ListPrepend(p, _) => path(p)
      case SetOperand.IfNotExists(p, _) => path(p)
    }

  private def key(kce: Option[KeyConditionExpr[_]]): List[String] = kce.fold(List.empty[String])(key)

  private def key(kce: KeyConditionExpr[_]): List[String] =
    kce match {
      case k: KeyConditionExpr.PartitionKeyEquals[_]              => k.pk.invalidPath.toList
      case k: KeyConditionExpr.CompositePrimaryKeyExpr[_]         =>
        k.pk.pk.invalidPath.toList ++ k.sk.sortKey.invalidPath.toList
      case k: KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[_] => k.pk.pk.invalidPath.toList ++ sortKey(k.sk)
      case _: KeyConditionExpr.Failure[_]                         => Nil
    }

  private def sortKey(sk: ExtendedSortKeyExpr[_, _]): List[String] =
    sk match {
      case e: ExtendedSortKeyExpr.GreaterThan[_, _]        => e.sortKey.invalidPath.toList
      case e: ExtendedSortKeyExpr.LessThan[_, _]           => e.sortKey.invalidPath.toList
      case e: ExtendedSortKeyExpr.NotEqual[_, _]           => e.sortKey.invalidPath.toList
      case e: ExtendedSortKeyExpr.LessThanOrEqual[_, _]    => e.sortKey.invalidPath.toList
      case e: ExtendedSortKeyExpr.GreaterThanOrEqual[_, _] => e.sortKey.invalidPath.toList
      case e: ExtendedSortKeyExpr.Between[_, _]            => e.left.invalidPath.toList
      case e: ExtendedSortKeyExpr.BeginsWith[_, _]         => e.left.invalidPath.toList
    }
}
