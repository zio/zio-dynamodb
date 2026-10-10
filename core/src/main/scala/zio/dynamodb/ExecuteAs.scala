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
 * Mixed into an interpreter whose `run` returns a narrower type than its `Interpreter[F]`'s
 * `F[A]` (e.g. a ZIO interpreter with a typed error channel). `.execute` picks this up from the
 * interpreter's static type; an interpreter only known as `Interpreter[F]` still yields `F[A]`.
 */
trait NarrowedResult {
  type Result[A]
  def run[Out](query: DynamoDBQuery[_, Out]): Result[Out]

  /** Runs a query producing `Either[ConditionFailed, Out]` with the `Left` moved into `Result`'s error channel. */
  def runFlattened[Out](query: DynamoDBQuery[_, Either[DynamoDBError.ItemError.ConditionFailed, Out]]): Result[Out]

  private[dynamodb] final val flattening: ExecuteAs.Flattening = new ExecuteAs.Flattening(this)
}

/**
 * Resolves what `.execute` returns for a query producing `Out`. A value class over the
 * interpreter, so resolving it allocates nothing.
 */
final class ExecuteAs[Out, R](private val interpreter: AnyRef) extends AnyVal {
  def apply(query: DynamoDBQuery[_, Out]): R =
    interpreter match {
      case f: ExecuteAs.Flattening =>
        f.interpreter
          .runFlattened(query.asInstanceOf[DynamoDBQuery[_, Either[DynamoDBError.ItemError.ConditionFailed, Any]]])
          .asInstanceOf[R]
      case n: NarrowedResult       => n.run(query).asInstanceOf[R]
      case i                       => i.asInstanceOf[Interpreter[ExecuteAs.Id]].run(query).asInstanceOf[R]
    }
}

object ExecuteAs extends ExecuteAsNarrowed {
  private[dynamodb] type Id[A] = A

  // Allocated once per interpreter; marks "flatten the Either" without a per-call allocation.
  private[dynamodb] final class Flattening(val interpreter: NarrowedResult)

  implicit def narrowedFlattened[A, J <: NarrowedResult](implicit
    j: J
  ): ExecuteAs[Either[DynamoDBError.ItemError.ConditionFailed, A], j.Result[A]] =
    new ExecuteAs(j.flattening)
}

trait ExecuteAsNarrowed extends ExecuteAsLowPriority {
  implicit def narrowed[Out, J <: NarrowedResult](implicit j: J): ExecuteAs[Out, j.Result[Out]] =
    new ExecuteAs(j)
}

trait ExecuteAsLowPriority {
  implicit def plain[Out, F[_]](implicit i: Interpreter[F]): ExecuteAs[Out, F[Out]] =
    new ExecuteAs(i)
}
