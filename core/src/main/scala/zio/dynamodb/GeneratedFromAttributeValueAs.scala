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

import zio.dynamodb.DynamoDBError.ItemError.DecodingError

private[dynamodb] trait GeneratedFromAttributeValueAs { this: AttrMap =>

  def as[A: FromAttributeValue, B: FromAttributeValue, C](
    field1: String,
    field2: String
  )(fn: (A, B) => C): Either[DecodingError, C] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
    } yield fn(a, b)

  def as[A: FromAttributeValue, B: FromAttributeValue, C: FromAttributeValue, D](
    field1: String,
    field2: String,
    field3: String
  )(fn: (A, B, C) => D): Either[DecodingError, D] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
    } yield fn(a, b, c)

  def as[A: FromAttributeValue, B: FromAttributeValue, C: FromAttributeValue, D: FromAttributeValue, E](
    field1: String,
    field2: String,
    field3: String,
    field4: String
  )(fn: (A, B, C, D) => E): Either[DecodingError, E] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
    } yield fn(a, b, c, d)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String
  )(fn: (A, B, C, D, E) => F): Either[DecodingError, F] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
    } yield fn(a, b, c, d, e)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String
  )(fn: (A, B, C, D, E, F) => G): Either[DecodingError, G] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
    } yield fn(a, b, c, d, e, f)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String
  )(fn: (A, B, C, D, E, F, G) => H): Either[DecodingError, H] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
    } yield fn(a, b, c, d, e, f, g)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String
  )(fn: (A, B, C, D, E, F, G, H) => I): Either[DecodingError, I] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
    } yield fn(a, b, c, d, e, f, g, h)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String
  )(fn: (A, B, C, D, E, F, G, H, I) => J): Either[DecodingError, J] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
    } yield fn(a, b, c, d, e, f, g, h, i)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String
  )(fn: (A, B, C, D, E, F, G, H, I, J) => K): Either[DecodingError, K] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
    } yield fn(a, b, c, d, e, f, g, h, i, j)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K) => L): Either[DecodingError, L] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L) => M): Either[DecodingError, M] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M) => N): Either[DecodingError, N] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => O): Either[DecodingError, O] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => P): Either[DecodingError, P] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P: FromAttributeValue,
    Q
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String,
    field16: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => Q): Either[DecodingError, Q] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
      p <- get[P](field16)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P: FromAttributeValue,
    Q: FromAttributeValue,
    R
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String,
    field16: String,
    field17: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => R): Either[DecodingError, R] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
      p <- get[P](field16)
      q <- get[Q](field17)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P: FromAttributeValue,
    Q: FromAttributeValue,
    R: FromAttributeValue,
    S
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String,
    field16: String,
    field17: String,
    field18: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => S): Either[DecodingError, S] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
      p <- get[P](field16)
      q <- get[Q](field17)
      r <- get[R](field18)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P: FromAttributeValue,
    Q: FromAttributeValue,
    R: FromAttributeValue,
    S: FromAttributeValue,
    T
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String,
    field16: String,
    field17: String,
    field18: String,
    field19: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) => T): Either[DecodingError, T] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
      p <- get[P](field16)
      q <- get[Q](field17)
      r <- get[R](field18)
      s <- get[S](field19)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P: FromAttributeValue,
    Q: FromAttributeValue,
    R: FromAttributeValue,
    S: FromAttributeValue,
    T: FromAttributeValue,
    U
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String,
    field16: String,
    field17: String,
    field18: String,
    field19: String,
    field20: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) => U): Either[DecodingError, U] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
      p <- get[P](field16)
      q <- get[Q](field17)
      r <- get[R](field18)
      s <- get[S](field19)
      t <- get[T](field20)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P: FromAttributeValue,
    Q: FromAttributeValue,
    R: FromAttributeValue,
    S: FromAttributeValue,
    T: FromAttributeValue,
    U: FromAttributeValue,
    V
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String,
    field16: String,
    field17: String,
    field18: String,
    field19: String,
    field20: String,
    field21: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) => V): Either[DecodingError, V] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
      p <- get[P](field16)
      q <- get[Q](field17)
      r <- get[R](field18)
      s <- get[S](field19)
      t <- get[T](field20)
      u <- get[U](field21)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)

  def as[
    A: FromAttributeValue,
    B: FromAttributeValue,
    C: FromAttributeValue,
    D: FromAttributeValue,
    E: FromAttributeValue,
    F: FromAttributeValue,
    G: FromAttributeValue,
    H: FromAttributeValue,
    I: FromAttributeValue,
    J: FromAttributeValue,
    K: FromAttributeValue,
    L: FromAttributeValue,
    M: FromAttributeValue,
    N: FromAttributeValue,
    O: FromAttributeValue,
    P: FromAttributeValue,
    Q: FromAttributeValue,
    R: FromAttributeValue,
    S: FromAttributeValue,
    T: FromAttributeValue,
    U: FromAttributeValue,
    V: FromAttributeValue,
    W
  ](
    field1: String,
    field2: String,
    field3: String,
    field4: String,
    field5: String,
    field6: String,
    field7: String,
    field8: String,
    field9: String,
    field10: String,
    field11: String,
    field12: String,
    field13: String,
    field14: String,
    field15: String,
    field16: String,
    field17: String,
    field18: String,
    field19: String,
    field20: String,
    field21: String,
    field22: String
  )(fn: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) => W): Either[DecodingError, W] =
    for {
      a <- get[A](field1)
      b <- get[B](field2)
      c <- get[C](field3)
      d <- get[D](field4)
      e <- get[E](field5)
      f <- get[F](field6)
      g <- get[G](field7)
      h <- get[H](field8)
      i <- get[I](field9)
      j <- get[J](field10)
      k <- get[K](field11)
      l <- get[L](field12)
      m <- get[M](field13)
      n <- get[N](field14)
      o <- get[O](field15)
      p <- get[P](field16)
      q <- get[Q](field17)
      r <- get[R](field18)
      s <- get[S](field19)
      t <- get[T](field20)
      u <- get[U](field21)
      v <- get[V](field22)
    } yield fn(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
}
