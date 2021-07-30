package zio.dynamodb

import zio.ZIO
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.stream.ZStream

object BatchFromStream {

  def batchWriteFromStream[R, A](
    mPar: Int, // TODO make last param
    stream: ZStream[R, Exception, A]
  )(f: A => DynamoDBQuery.Write[Unit]): ZIO[DynamoDBExecutor with R, Exception, Unit] =
    stream
      .grouped(25)
      .mapMPar(mPar) { chunk =>
        val zio = DynamoDBQuery
          .forEach(chunk)(a => f(a))
        for {
          r <- ZIO.environment[DynamoDBExecutor]
          _ <- zio.execute.provide(r)
        } yield ()
      }
      .runDrain

  def batchReadFromStream[R, A](
    tableName: String,
    mPar: Int = 4, // TODO make last param
    stream: ZStream[R, Exception, A]
  )(
    pk: A => PrimaryKey
  )(f: List[Option[Item]] => ZIO[R, Exception, Unit]): ZIO[R with DynamoDBExecutor, Exception, Unit] =
    stream
      .grouped(100)
      .mapMPar(mPar) { chunk =>
        val zio: DynamoDBQuery[List[Option[Item]]] = DynamoDBQuery
          .forEach(chunk)(a => DynamoDBQuery.getItem(tableName, pk(a)))
        for {
          r    <- ZIO.environment[DynamoDBExecutor]
          list <- zio.execute.provide(r)
          _    <- f(list)
        } yield ()
      }
      .runDrain

}
