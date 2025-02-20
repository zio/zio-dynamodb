package zio.dynamodb.examples

import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery.put
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.{ UStream, ZStream }
import zio.ZIOAppDefault
import zio.Console.printLine
import zio.ZIO
import zio.dynamodb.batchWriteFromStream2
import zio.dynamodb.DynamoDBQuery.deleteFrom

object BatchFromStreamExamples extends ZIOAppDefault {

  final case class Person(id: Int, name: String)
  object Person {
    implicit val schema: Schema.CaseClass2[Int, String, Person] = DeriveSchema.gen[Person]
    val (id, name)                                              = ProjectionExpression.accessors[Person]
  }

  private val personIdStream: UStream[Int] =
    ZStream.fromIterable(1 to 20)

  private val personStream: UStream[Person] =
    ZStream.fromIterable(1 to 20).map(i => Person(i, s"name$i"))

  override def run: ZIO[Any, Throwable, Unit] =
    (for {
      _ <- TestDynamoDBExecutor.addTable("person", "id")
      // write to DB using the stream as the source of the data to write
      // note put query uses type safe API to save a Person case class directly using Schema derived codecs
      // write queries will automatically be batched using BatchWriteItem when calling DynamoDB
      _ <- batchWriteFromStream2(personStream) { person =>
             put("person", Person(person.id, person.name))
           }.runDrain

      // read from the DB using the stream as the source of the primary key
      // read queries will automatically be batched using BatchGetItem when calling DynamoDB
      _ <- batchReadItemFromStream("person", personIdStream)(id => PrimaryKey("id" -> id))
             .mapZIOPar(4)(item => printLine(s"item=$item"))
             .runDrain

      _ <- batchWriteFromStream(personStream) {
             person => // TODO: Avi - DeleteItem extends HasNoCondition + batchWriteFromStream2
               deleteFrom("person")(Person.id.partitionKey === person.id)
           }.runDrain

      _ <- batchReadItemFromStream("person", personIdStream)(id => PrimaryKey("id" -> id))
             .mapZIOPar(4)(item => printLine(s"item=$item"))
             .runDrain

    } yield ()).provideLayer(DynamoDBExecutor.test)
}
