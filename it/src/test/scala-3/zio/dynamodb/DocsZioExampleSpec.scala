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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.*
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*
import zio.test.*

/**
 * Mirrors docs/index.md's "See it in action" (ZIO) snippet exactly, as a compile-time check
 *  of that precise shape — a class/method body is type-checked whether or not it's ever
 *  instantiated/called, so this fails the build if the doc's code stops compiling. Not run
 *  against a real client (no Docker/Testcontainers dependency); the underlying
 *  ZioInterpreter + dsl facade + .execute mechanics are already exercised for real
 *  elsewhere (ZioInterpreterSpec and friends).
 *
 *  Lives in `it` rather than `zio` because zioInterpreter doesn't depend on schemaDdbExpr
 *  (only ceInterpreter does, for Test) — the HL dsl facade isn't on zioInterpreter's
 *  classpath.
 */
object DocsZioExampleObject extends ZIOAppDefault {

  enum Genre derives Schema {
    case Drama, Comedy
  }

  case class Movie(id: String, genre: Genre) derives Schema

  object Movie extends CompanionOptics[Movie] {
    val id: Lens[Movie, String]   = $(_.id)
    val genre: Lens[Movie, Genre] = $(_.genre)
  }

  val movies = Table[Movie]("movies")

  // ZLayer.scoped ties the client's lifetime to the layer's scope — closed automatically
  // when `program.provide(interpreterLayer)` finishes, no matter how it finishes. The
  // release action must be a URIO (cannot fail), unlike Resource's CE-effect release.
  val interpreterLayer: ZLayer[Any, Throwable, Interpreter[Task]] =
    ZLayer.scoped {
      ZIO
        .acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        .map(client => ZioInterpreter.fromAsyncClient(client): Interpreter[Task])
    }

  // .execute takes the interpreter as a plain implicit, not via ZIO's R environment — so
  // the layer's service gets pulled out with ZIO.serviceWithZIO and bound as a `given`
  // for the body, rather than the query itself living in the ZIO environment.
  val program: ZIO[Interpreter[Task], Throwable, Unit] =
    ZIO.serviceWithZIO[Interpreter[Task]] { interpreter =>
      given Interpreter[Task] = interpreter
      for {
        _     <- put(movies, Movie("m1", Genre.Drama)).execute
        movie <- get(movies)(Movie.id.partitionKey === "m1").execute
        page  <- scan(movies, 20).filter(Movie.genre === Genre.Drama).execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}

object DocsZioExampleSpec extends ZIOSpecDefault {
  def spec = suite("DocsZioExampleSpec")(
    test("docs/index.md ZIO example compiles") {
      // Compilation of DocsZioExampleObject above is the actual check; this just gives
      // the compile-check a visible place in test output.
      assertTrue(true)
    }
  )
}
