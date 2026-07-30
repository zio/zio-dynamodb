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

import java.util.concurrent.ConcurrentHashMap
import zio.blocks.chunk.Chunk
import zio.blocks.schema.{ Schema, SchemaExpr }
import zio.dynamodb._
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

/**
 * High-level CRUD API backed by [[DdbExpr]] condition expressions and [[DdbKeyExpr]]
 *  key condition expressions.
 *
 *  Unlike the deprecated [[zio.dynamodb.blocks.DdbSchemaExprApi]], all ZB [[zio.blocks.schema.Optic]]
 *  operators (===, >, <, >=, <=) encode sealed-trait literals correctly. Since zio-blocks v0.0.47
 *  [[zio.blocks.schema.DynamicSchemaExpr.Literal]] carries a [[zio.blocks.schema.Schema]], the
 *  interpreter derives a [[zio.dynamodb.blocks.schema.DynamoDBCodec]] at evaluation time —
 *  `enumValuesAsStrings` and other encoding rules are preserved automatically.
 *
 *  Usage:
 *  {{{
 *    import DdbExprApi._
 *    import DdbKeyExpr._
 *    import DdbExpr.{ DdbExprBoolSyntax, OpticDdbExprOps, OpticStringDdbExprOps, SchemaExprBoolBridge }
 *
 *    // CRUD
 *    DdbExprApi.put("tasks", task)
 *    DdbExprApi.get[Task]("tasks")(Task.id.partitionKey === "t1")
 *    DdbExprApi.deleteFrom[Task]("tasks")(Task.id.partitionKey === "t1")
 *
 *    // scalars and sealed traits — ZB Optic operators, lifted to Builtin
 *    DdbExprApi.scan[Task]("tasks", 20).filter(Task.score > 0)
 *    DdbExprApi.scan[Task]("tasks", 20).filter(Task.priority === Priority.High)
 *
 *    // DDB functions + combinators
 *    DdbExprApi.query[Task]("tasks", 20)
 *      .whereKey(Task.id.partitionKey === "alice" && Task.score.sortKey > 10)
 *      .filter(Task.name.beginsWith("A") && Task.score.between(1, 100))
 *  }}}
 *
 *  Importing [[DdbExprApi]]`._ ` brings the implicit conversions
 *  [[ddbKeyExprToKeyConditionExpr]], [[ddbExprToConditionExpression]], and
 *  [[schemaExprToConditionExpression]] into scope, enabling `.whereKey(DdbKeyExpr)`,
 *  `.filter(DdbExpr)`, and `.filter(SchemaExpr)` on any [[DynamoDBQuery]].
 *  Interpretation failures are deferred to query execution via the `Failure` nodes in
 *  [[KeyConditionExpr]] and [[ConditionExpression]].
 */
object DdbExprApi {

  // ── Codec cache ───────────────────────────────────────────────────────────────

  private case class CodecEntry[A](codec: DynamoDBCodec[A], projections: Chunk[ProjectionExpression[_, _]])

  // Keyed by (Schema, DynamoDBCodecDeriverConfigure) reference identity — same
  // rationale as DdbSchemaExprApi: avoids cross-classloader collisions and ensures
  // types with custom configures get their own entry.
  private final class CodecCacheKey(private val r0: AnyRef, private val r1: AnyRef) {
    override val hashCode: Int           = System.identityHashCode(r0) * 31 + System.identityHashCode(r1)
    override def equals(o: Any): Boolean = o match {
      case k: CodecCacheKey => (r0 eq k.r0) && (r1 eq k.r1)
      case _                => false
    }
  }
  private val codecCache = new ConcurrentHashMap[CodecCacheKey, CodecEntry[_]]()

  private def cachedEntry[A](implicit schema: Schema[A], cfg: DynamoDBCodecDeriverConfigure[A]): CodecEntry[A] = {
    val key      = new CodecCacheKey(schema, cfg)
    val existing = codecCache.get(key)
    if (existing != null) existing.asInstanceOf[CodecEntry[A]]
    else {
      val codec       = schema.deriving(cfg.configure(DynamoDBCodecDeriver)).derive
      val projections = codec.recordFieldNames.map(name =>
        ProjectionExpression.MapElement(ProjectionExpression.Root, name): ProjectionExpression[_, _]
      )
      val entry       = CodecEntry(codec, projections)
      codecCache.putIfAbsent(key, entry)
      codecCache.get(key).asInstanceOf[CodecEntry[A]]
    }
  }

  // ── Item helpers ──────────────────────────────────────────────────────────────

  private[ddbexpr] def fromItem[A](item: Item)(codec: DynamoDBCodec[A]): Either[DynamoDBError.ItemError, A] = {
    val av = ToAttributeValue.attrMapToAttributeValue.toAttributeValue(item)
    codec.decoder(av)
  }

  private def toItem[A](a: A)(codec: DynamoDBCodec[A]): Either[DynamoDBError, Item] =
    FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue(codec.encoder(a))

  // ── CRUD operations ───────────────────────────────────────────────────────────

  def put[A](tableName: String, a: A)(implicit
    schema: Schema[A],
    cfg: DynamoDBCodecDeriverConfigure[A]
  ): DynamoDBQuery[A, Option[A]] = {
    val codec = cachedEntry[A].codec
    toItem(a)(codec) match {
      case Right(encodedItem) =>
        DynamoDBQuery
          .putItem(tableName, encodedItem)
          .map(_.flatMap(prevItem => fromItem[A](prevItem)(codec).toOption))
      case Left(err)          =>
        DynamoDBQuery.fail(err)
    }
  }

  def get[From](tableName: String)(keyExpr: DdbKeyExpr.PrimaryKey[From])(implicit
    schema: Schema[From],
    cfg: DynamoDBCodecDeriverConfigure[From]
  ): DynamoDBQuery[From, Either[DynamoDBError.ItemError, From]] = {
    val entry = cachedEntry[From]
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr) match {
      case Right(pkExpr) =>
        val pkAttrMap = pkExpr.asAttrMap
        DynamoDBQuery.getItem(tableName, pkAttrMap, entry.projections: _*).map {
          case Some(item) => fromItem[From](item)(entry.codec)
          case None       => Left(DynamoDBError.ItemError.ValueNotFound(s"value with key $pkAttrMap not found"))
        }
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }
  }

  def update[From](tableName: String)(keyExpr: DdbKeyExpr.PrimaryKey[From])(
    action: UpdateExpression.Action[From]
  )(implicit
    schema: Schema[From],
    cfg: DynamoDBCodecDeriverConfigure[From]
  ): DynamoDBQuery[From, Option[From]] = {
    val codec = cachedEntry[From].codec
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr) match {
      case Right(pkExpr) =>
        DynamoDBQuery
          .updateItem(tableName, pkExpr.asAttrMap)(action)
          .map(_.flatMap(item => fromItem[From](item)(codec).toOption))
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }
  }

  def deleteFrom[From](tableName: String)(keyExpr: DdbKeyExpr.PrimaryKey[From])(implicit
    schema: Schema[From],
    cfg: DynamoDBCodecDeriverConfigure[From]
  ): DynamoDBQuery[From, Option[From]] = {
    val codec = cachedEntry[From].codec
    DdbKeyExprInterpreter.toPrimaryKeyExpr(keyExpr) match {
      case Right(pkExpr) =>
        DynamoDBQuery
          .deleteItem(tableName, pkExpr.asAttrMap)
          .map(_.flatMap(item => fromItem[From](item)(codec).toOption))
      case Left(msg)     =>
        DynamoDBQuery.fail(DynamoDBError.ItemError.DecodingError.failure(msg))
    }
  }

  // query and scan return a base query; callers chain .whereKey(DdbKeyExpr) and
  // .filter(DdbExpr) via the implicit conversions below.
  def query[From](tableName: String, limit: Int)(implicit
    schema: Schema[From],
    cfg: DynamoDBCodecDeriverConfigure[From]
  ): DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] = {
    val entry = cachedEntry[From]
    DynamoDBQuery
      .querySome(tableName, limit)
      .map(page =>
        Page(
          items = page.items.map(item => fromItem[From](item)(entry.codec)),
          lastEvaluatedKey = page.lastEvaluatedKey,
          count = page.count,
          scannedCount = page.scannedCount
        )
      )
  }

  def scan[From](tableName: String, limit: Int)(implicit
    schema: Schema[From],
    cfg: DynamoDBCodecDeriverConfigure[From]
  ): DynamoDBQuery[From, Page[Either[DynamoDBError.ItemError, From]]] = {
    val entry = cachedEntry[From]
    DynamoDBQuery
      .scanSome(tableName, limit)
      .map(page =>
        Page(
          items = page.items.map(item => fromItem[From](item)(entry.codec)),
          lastEvaluatedKey = page.lastEvaluatedKey,
          count = page.count,
          scannedCount = page.scannedCount
        )
      )
  }

  // ── Implicit conversions ──────────────────────────────────────────────────────

  // Enables .whereKey(ddbKeyExpr) on any DynamoDBQuery.
  // Interpretation failures are deferred to execution via KeyConditionExpr.Failure.
  implicit def ddbKeyExprToKeyConditionExpr[S](expr: DdbKeyExpr[S]): KeyConditionExpr[S] =
    DdbKeyExprInterpreter.toKeyConditionExpr(expr).fold(KeyConditionExpr.Failure(_), identity)

  // Enables .filter(ddbExpr) and .where(ddbExpr) on any DynamoDBQuery.
  // FilterExpression[-From] is a type alias for ConditionExpression[-From].
  // Interpretation failures are deferred to execution via ConditionExpression.Failure.
  implicit def ddbExprToConditionExpression[S](expr: DdbExpr[S, Boolean]): ConditionExpression[S] =
    DdbExprInterpreter.toConditionExpression(expr).fold(ConditionExpression.Failure(_), identity)

  // Enables .filter(Task.score > 0) or .filter(Task.priority === Priority.High) where
  // the argument is a ZB SchemaExpr. Goes through the Builtin path; the interpreter
  // derives a DynamoDBCodec from the embedded Schema[_] in each Literal node.
  implicit def schemaExprToConditionExpression[S](se: SchemaExpr[S, Boolean]): ConditionExpression[S] =
    ddbExprToConditionExpression(DdbExpr.Builtin(se))
}
