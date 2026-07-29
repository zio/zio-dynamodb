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

private[dynamodb] final case class NonEmptySet[A] private (private val set: Set[A]) extends Iterable[A] {
  self =>
  def +(a: A): NonEmptySet[A]               = new NonEmptySet(set + a)
  def ++(that: Iterable[A]): NonEmptySet[A] = new NonEmptySet(set ++ that)

  override def iterator: Iterator[A] = set.iterator

}
private[dynamodb] object NonEmptySet {
  def apply[A](head: A, tail: Set[A]): NonEmptySet[A] = new NonEmptySet[A](tail + head)
  def apply[A](head: A, tail: A*): NonEmptySet[A]     = apply(head, tail.toSet)
}
