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
import zio.blocks.schema.{ DynamicOptic, Optic }
import zio.dynamodb.blocks.CodecNavigator
import zio.dynamodb.blocks.schema.{ DerivedCodec, DynamoDBCodec }

/**
 * Per-[[Table]] expression-resolution context, built once per table. Resolves an optic against the
 * table's own codec - the one `put` writes with - so an expression's attribute paths and literal
 * encodings always match the stored item.
 */
private[ddbexpr] final class ExprCtx(private[ddbexpr] val codec: DynamoDBCodec[_]) {

  // A hand-written top-level codec can't be navigated, so every path - key conditions included - fails
  // with this one error, checked once per table. `put` and unfiltered scans only encode and decode.
  private[this] val opaque: Left[String, Nothing] =
    if (codec.isInstanceOf[DerivedCodec[_]]) null
    else
      Left(
        "the table's codec was not derived by DynamoDBCodecDeriver, so expression paths can't be resolved " +
          "against it - derive it with DynamoDBCodecDeriver and override single fields with .instance(optic, codec)"
      )

  // Keyed by DynamicOptic alone: the codec is fixed per table.
  private[this] val cache = new ConcurrentHashMap[DynamicOptic, Either[String, CodecNavigator.Resolved]]()

  private[ddbexpr] def at(optic: Optic[_, _]): Either[String, CodecNavigator.Resolved] = at(optic.toDynamic)

  private[ddbexpr] def at(dyn: DynamicOptic): Either[String, CodecNavigator.Resolved] = {
    val hit = cache.get(dyn)
    if (hit ne null) hit
    else if (opaque ne null) opaque
    else {
      val computed = CodecNavigator.resolve(codec, dyn)
      cache.putIfAbsent(dyn, computed)
      computed
    }
  }

  // A key optic is a single top-level field: one step on the root codec, which is a prebuilt
  // lookup, so key conditions skip the cache and allocate nothing.
  private[ddbexpr] def topLevelField(field: DynamicOptic.Node.Field): Either[String, DynamoDBCodec.Step] =
    if (opaque ne null) opaque else codec.step(field)

  // The codec for an element of the collection at `resolved` - list, set or map values.
  private[ddbexpr] def elementCodec(resolved: CodecNavigator.Resolved): Either[String, DynamoDBCodec[Any]] =
    resolved.codec.step(DynamicOptic.Node.Elements) match {
      case Right(step) => Right(step.codec)
      case Left(_)     => Left(s"no element codec at '${resolved.path}'")
    }
}
