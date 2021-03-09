package zio.dynamodb

import zio.dynamodb.DynamoDBQuery._
import zio.{ Chunk, Has, ZIO, ZLayer }
import scala.collection.immutable.{ Map => ScalaMap }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  def batchGetItems[A](query: DynamoDBQuery[A]): DynamoDBQuery[A] =
    query match {
      case Zip(left, right) =>
        (left, right) match {
          case (getItemLeft @ GetItem(_, _, _, _, _), getItemRight @ GetItem(_, _, _, _, _))         =>
            val batch = (BatchGetItem(ScalaMap.empty) + getItemLeft) + getItemRight
            batchGetItems(batch.asInstanceOf[DynamoDBQuery[A]])
          case (Zip(x, getItemLeft @ GetItem(_, _, _, _, _)), getItemRight @ GetItem(_, _, _, _, _)) =>
            batchGetItems(Zip(x, (BatchGetItem(ScalaMap.empty) + getItemRight) + getItemLeft))
          case (getItemLeft @ GetItem(_, _, _, _, _), batchRight @ BatchGetItem(_, _))               =>
            (batchRight + getItemLeft).asInstanceOf[DynamoDBQuery[A]]
          case (Zip(x, getItemLeft @ GetItem(_, _, _, _, _)), batchRight @ BatchGetItem(_, _))       =>
            batchGetItems(Zip(x, batchRight + getItemLeft))
          case _                                                                                     =>
            Zip(batchGetItems(left), batchGetItems(right))
        }
      case other            =>
        other
    }

  private[dynamodb] def parallelize[A](query: DynamoDBQuery[A]): (Chunk[Constructor[Any]], Chunk[Any] => A) =
    query match {
      case Map(query, mapper) =>
        parallelize(query) match {
          case (constructors, assembler) =>
            (constructors, assembler.andThen(mapper))
        }

      case Zip(left, right)   =>
        val (constructorsLeft, assemblerLeft)   = parallelize(left)
        val (constructorsRight, assemblerRight) = parallelize(right)
        (
          constructorsLeft ++ constructorsRight,
          (results: Chunk[Any]) => {
            val (leftResults, rightResults) = results.splitAt(constructorsLeft.length)
            val left                        = assemblerLeft(leftResults)
            val right                       = assemblerRight(rightResults)
            (left, right).asInstanceOf[A]
          }
        )

      case Succeed(value)     => (Chunk.empty, _ => value.asInstanceOf[A])

      case getItem @ GetItem(_, _, _, _, _)           =>
        (
          Chunk(getItem),
          (results: Chunk[Any]) => {
            println(s"GetItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case putItem @ PutItem(_, _, _, _, _, _)        =>
        (
          Chunk(putItem),
          (results: Chunk[Any]) => {
            println(s"PutItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case deleteItem @ DeleteItem(_, _, _, _, _, _)  =>
        (
          Chunk(deleteItem),
          (results: Chunk[Any]) => {
            println(s"DeleteItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case scanItem @ Scan(_, _, _, _, _, _, _, _, _) =>
        (
          Chunk(scanItem),
          (results: Chunk[Any]) => {
            println(s"Scan results=$results")
            results.head.asInstanceOf[A]
          }
        )

      // TODO: put, delete
      // TODO: scan, query

      case _                                          =>
        (Chunk.empty, _ => ().asInstanceOf[A]) //TODO: remove
    }

  def live =
    ZLayer.fromService[DynamoDb.Service, DynamoDBExecutor.Service](dynamoDb =>
      new Service {
        override def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
          val (constructors, assembler) = parallelize(query)

          for {
            chunks   <- ZIO.foreach(constructors)(dynamoDb.execute)
            assembled = assembler(chunks)
          } yield assembled
        }
      }
    )

}
