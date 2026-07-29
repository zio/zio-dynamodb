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

object Utils {
  // move Prelude ops here to maintain zero dependency
  implicit class ListUtils[E, A, B](list: Iterable[A]) {
    def forEach(f: A => Either[E, B]): Either[E, Iterable[B]] = {
      val buf = List.newBuilder[B]
      val it  = list.iterator
      while (it.hasNext)
        f(it.next()) match {
          case Left(e)  => return Left(e)
          case Right(b) => buf += b
        }
      Right(buf.result())
    }

    def reverse: Iterable[A] = {
      var result: List[A] = Nil
      val it              = list.iterator
      while (it.hasNext)
        result = it.next() :: result
      result
    }
  }

}
