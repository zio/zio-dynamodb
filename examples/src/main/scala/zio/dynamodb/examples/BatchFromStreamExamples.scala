package zio.dynamodb.examples

import zio.console.{ putStrLn, Console }
import zio.dynamodb.DynamoDBQuery.putItem
import zio.dynamodb.{
  batchReadFromStream,
  batchWriteFromStream,
  DynamoDBExecutor,
  Item,
  PrimaryKey,
  TestDynamoDBExecutor
}
import zio.stream.{ UStream, ZStream }
import zio.{ App, ExitCode, URIO }

object BatchFromStreamExamples extends App {

  private final case class Person(id: Int, name: String)

  private val executorLayer = DynamoDBExecutor.test

  private val personStream: UStream[Person] =
    ZStream.fromIterable(1 to 100).map(i => Person(i, s"name$i"))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- TestDynamoDBExecutor.addTable("table1", "id")()
      // write to DB using the stream as the source of the data to write
      // write queries will automatically be batched using BatchWriteItem when calling DynamoDB
      _ <- batchWriteFromStream(personStream) { person =>
             putItem("table1", Item("id" -> person.id, "name" -> person.name))
           }.runDrain
      // read from the DB using the stream as the source of the primary key
      // read queries will automatically be batched using BatchGetItem when calling DynamoDB
      _ <- batchReadFromStream[Console, Person]("table1", personStream)(person => PrimaryKey("id" -> person.id))
             .mapMPar(4)(item => putStrLn(s"item=$item"))
             .runDrain
    } yield ()).provideCustomLayer(executorLayer).exitCode
}
