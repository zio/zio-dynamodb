package zio.dynamodb.examples

import zio.console.{ putStrLn, Console }
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.{ putItem, scanAll }
import zio.dynamodb.fake.{ Database, FakeDynamoDBExecutor }
import zio.dynamodb.{ BatchFromStream, Item, PrimaryKey }
import zio.stream.ZStream
import zio.{ App, ExitCode, URIO, ZIO }

object BatchFromStreamExamples extends App {
  private val executorLayer = FakeDynamoDBExecutor.layer(Database().table("table1", "id")())

  final case class Person(id: Int, name: String)

  private val personStream: ZStream[Any, Nothing, Person] =
    ZStream.fromIterable(1 to 100).map(i => Person(i, s"name$i"))

  // read stream and do batched writes (grouped by 25 items) to Fake DDB
  private val batchWriteProgram: ZIO[Console with DynamoDBExecutor, Exception, Unit] =
    for {
      _      <- BatchFromStream
                  .batchWriteFromStream(personStream) { person =>
                    putItem("table1", Item("id" -> person.id, "name" -> person.name))
                  }
                  .runDrain
      stream <- scanAll("table1", "id").execute
      _      <- stream.tap(person => putStrLn(s"person=$person")).runDrain
    } yield ()

  // read stream to create batched GetItems (grouped by 100 items) which are executed against Fake DDB
  private val batchReadProgram: ZIO[Console with DynamoDBExecutor, Exception, Unit] =
    for {
      // TODO: why does batchReadFromStream not infer automatically?
      _ <- BatchFromStream
             .batchReadFromStream[Console, Person]("table1", personStream)(person => PrimaryKey("id" -> person.id))
             .mapMPar(4)(item => putStrLn(s"item=$item"))
             .runDrain
    } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- batchWriteProgram
      _ <- batchReadProgram
    } yield ()).provideCustomLayer(executorLayer).exitCode
}
