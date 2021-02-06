package zio.dynamodb

import zio.{ Task, ZLayer }
import zio.blocking.Blocking

import java.io.IOException

trait DynamoDBExecutor  {
  def execute[A](query: DynamoDBQuery[A]): Task[A]
}
object DynamoDBExecutor {
  // TODO: Depend on `sttp`
  def live(): ZLayer[Blocking, IOException, DynamoDBExecutor] = ???
}
