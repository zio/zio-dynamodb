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

/**
 * Single-import facade over [[DdbExprApi]] (CRUD), [[DdbKeyExpr]] (partition/sort key
 *  expressions), and [[DdbExpr]] (condition/filter/update expressions) — the three objects
 *  every real call site ends up importing together anyway. Mirrors the pattern Cats
 *  (`cats.syntax.all`) and Doobie (`doobie.implicits`) use for the same problem: each piece
 *  of syntax lives in its own trait (`DdbExprApiSyntax`, `DdbKeyExprSyntax`, `DdbExprSyntax`),
 *  and this object just mixes all three in.
 *
 *  {{{
 *    import zio.dynamodb.blocks.ddbexpr.dsl._
 *
 *    DdbExprApi.put("tasks", task)
 *    DdbExprApi.get[Task]("tasks")(Task.id.partitionKey === "t1")
 *    DdbExprApi.scan[Task]("tasks", 20).filter(Task.score > 0 && Task.priority === Priority.High)
 *  }}}
 *
 *  `DdbExprSyntax` and `DdbKeyExprSyntax` each independently need a `DynamoDBCodec[A]` for any
 *  `A` with a `Schema[A]` in scope; both get it from the same inherited
 *  `DerivedCodecSyntax.derivedCodec` rather than each declaring their own copy, so mixing them
 *  together here doesn't hit the ambiguous-implicit error that importing `DdbExpr._` and
 *  `DdbKeyExpr._` together used to.
 *
 *  This is purely a convenience for the common case — `DdbExprApi`, `DdbKeyExpr`, and
 *  `DdbExpr` remain independently importable exactly as before for callers who want only
 *  one piece (e.g. a test exercising `DdbKeyExpr` in isolation).
 */
object dsl extends DdbExprApiSyntax with DdbKeyExprSyntax with DdbExprSyntax
