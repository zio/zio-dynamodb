package zio.dynamodb.examples

import zio.console.{ putStrLn, Console }
import zio.dynamodb.DynamoDBQuery.putItem
import zio.dynamodb.fake.FakeDynamoDBExecutor
import zio.dynamodb.{ BatchFromStream, Item, PrimaryKey }
import zio.stream.ZStream
import zio.{ App, ExitCode, URIO }

object BatchFromStreamExamples extends App {
  private val executorLayer = FakeDynamoDBExecutor.table("table1", "id")().layer

  final case class Person(id: Int, name: String)

  private val personStream: ZStream[Any, Nothing, Person] =
    ZStream.fromIterable(1 to 100).map(i => Person(i, s"name$i"))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- BatchFromStream
             .batchWriteFromStream(personStream) { person =>
               putItem("table1", Item("id" -> person.id, "name" -> person.name))
             }
             .runDrain
      _ <- BatchFromStream
             .batchReadFromStream[Console, Person]("table1", personStream)(person => PrimaryKey("id" -> person.id))
             .mapMPar(4)(item => putStrLn(s"item=$item"))
             .runDrain
    } yield ()).provideCustomLayer(executorLayer).exitCode
}
