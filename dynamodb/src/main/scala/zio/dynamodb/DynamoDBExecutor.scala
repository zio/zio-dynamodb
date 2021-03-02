package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ DeleteItem, GetItem, Map, PutItem, Zip }
//import zio.dynamodb.DynamoDb.DynamoDb
import zio.{ App, ExitCode, Has, URIO, ZIO, ZLayer }

import Predef.{ println, Map => ScalaMap }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  /*
  Assuming you can only zip together
  - GetItems with other GetItems
  - PutItems/DeleteItems with other PutItems/DeleteItems

  recurse query and breakdown into a List of atomic queries ie DynamoDBQuery[_]
  that would be all
  - GetItems - these would get folded over as a single BatchGetItem request
  - DeleteItem OR PutRequest - these would get folded over as a single BatchWriteItem request
  - for each atomic item in the list execute the query request and assemble the results
   */
  def loop[A](query: DynamoDBQuery[A], acc: List[DynamoDBQuery[A]]): List[DynamoDBQuery[Any]] =
    query match {
      case Zip(left, right) =>
        loop(left.asInstanceOf[DynamoDBQuery[A]], acc) ++ loop(right.asInstanceOf[DynamoDBQuery[A]], acc)
      case q                =>
        acc :+ q
    }

  def live =
    ZLayer.fromService[DynamoDb.Service, DynamoDBExecutor.Service](dynamoDb =>
      new Service {
        override def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
          val queries = loop(query, List.empty)

          // for now we do not aggregate/batch queries
          val x = ZIO.foreachPar(queries)(dynamoDb.executeQueryAgainstDdb)
          println(x)
          ZIO.succeed(().asInstanceOf[A]) // TODO
        }
      }
    )

  def executeQueryAgainstDdb[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
    atomicQuery match {
      // TODO: figure out how to do the mapping
      case Map(query, mapper)                                                                   =>
        println(s"$query $mapper")
        ZIO.succeed(().asInstanceOf[A]) //.map(mapper)

      case GetItem(key, tableName, readConsistency, projections, capacity)                      =>
        println(s"$key $tableName $readConsistency $projections $capacity")
        ZIO.succeed(Some(Item(ScalaMap.empty)))
      case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
        println(s"$tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
        ZIO.succeed(())
      case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
        println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
        ZIO.succeed(())
      case x                                                                                    =>
        ZIO.fail(new Exception(s"$x not implemented yet"))
    }

}
object DynamoDBExecutorExample extends App {
  val getItem1                                                = GetItem(key = PrimaryKey(ScalaMap.empty), tableName = TableName("T1"))
  val getItem2                                                = GetItem(key = PrimaryKey(ScalaMap.empty), tableName = TableName("T2"))
  val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2

  val putItem                                   = PutItem(tableName = TableName("T1"), item = Item(ScalaMap.empty))
  val deleteItem                                = DeleteItem(tableName = TableName("T2"), key = PrimaryKey(ScalaMap.empty))
  val zippedWrites: DynamoDBQuery[(Unit, Unit)] = putItem zip deleteItem

  val mixed: DynamoDBQuery[((Option[Item], Option[Item]), (Unit, Unit))] =
    zippedGets zip zippedWrites // should we even allow this?

  val queries: List[DynamoDBQuery[Any]] = DynamoDBExecutor.loop(mixed, List.empty)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val x: ZIO[Any, Exception, List[Any]] = ZIO.foreachPar(queries)(DynamoDBExecutor.executeQueryAgainstDdb)
    x.map(_.foreach(x => println(s"x=$x"))).exitCode
  }
}
