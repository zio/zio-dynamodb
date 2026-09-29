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
 * The three independent, optional interceptors a `fromAsyncClient` factory accepts, bundled
 *  into one value so attaching any combination of them doesn't require a combinatorial overload
 *  set. Each field defaults to `None` — a caller sets only what they want, named:
 *  `InterceptorConfig(retry = Some(myRetryInterceptor))`.
 *
 *  Deliberately excludes `defaultRetryPolicy`: that's actual control-flow policy (whether/how a
 *  call retries at all), not observability, and stays a separate `fromAsyncClient` parameter —
 *  bundling it here would conflate two things a caller reasons about differently.
 */
final case class InterceptorConfig[F[_]](
  response: Option[ResponseInterceptor[F]] = None,
  retry: Option[RetryInterceptor[F]] = None,
  batchRetry: Option[BatchRetryInterceptor[F]] = None
)
