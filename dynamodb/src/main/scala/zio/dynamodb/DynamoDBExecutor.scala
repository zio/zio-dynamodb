package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ BatchGetItem, BatchWriteItem, DeleteItem, GetItem, Map, PutItem, Zip }
import zio.dynamodb.DynamoDBQuery.BatchGetItem.{ TableItem }
import zio.dynamodb.DynamoDBQuery.BatchWriteItem.{ Delete, Put, Write, WriteItemsMap }
import zio.{ App, ExitCode, Has, URIO, ZIO, ZLayer }

import scala.collection.immutable.{ Map => ScalaMap }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  final case class Aggregated(
    batchGetItem: BatchGetItem = BatchGetItem(ScalaMap.empty),
    batchWriteItem: BatchWriteItem = BatchWriteItem(WriteItemsMap.empty),
    nonBatched: List[DynamoDBQuery[Any]] = List.empty
  ) { self =>
    private def toBatchGetElement(bi: GetItem): (TableName, TableItem)  =
      (bi.tableName, TableItem(bi.key, bi.projections))
    private def toBatchWriteElement(bw: PutItem): (TableName, Write)    =
      (bw.tableName, Put(bw.item))
    private def toBatchWriteElement(bw: DeleteItem): (TableName, Write) =
      (bw.tableName, Delete(bw.key))

    def addGetItem(gi: GetItem)       =
      Aggregated(
        BatchGetItem(self.batchGetItem.requestItems + toBatchGetElement(gi)),
        self.batchWriteItem,
        self.nonBatched
      )
    def addPutItem(pi: PutItem)       =
      Aggregated(
        self.batchGetItem,
        BatchWriteItem(self.batchWriteItem.requestItems + toBatchWriteElement(pi)),
        self.nonBatched
      )
    def addDeleteItem(di: DeleteItem) =
      Aggregated(
        self.batchGetItem,
        BatchWriteItem(self.batchWriteItem.requestItems + toBatchWriteElement(di)),
        self.nonBatched
      )

    def +[A](that: DynamoDBQuery[A]) = {
      val x = Aggregated(self.batchGetItem, self.batchWriteItem, self.nonBatched :+ that)
      x
    }

    def ++(that: Aggregated) =
      Aggregated(
        self.batchGetItem ++ that.batchGetItem,
        self.batchWriteItem ++ that.batchWriteItem,
        self.nonBatched.appendedAll(that.nonBatched)
      )
  }

  // aggregation using a custom Aggregated accumulator
  def loop[A](query: DynamoDBQuery[A], acc: Aggregated): Aggregated =
    query match {
      case Zip(left, right)                          =>
        loop(left.asInstanceOf[DynamoDBQuery[A]], acc) ++ loop(right.asInstanceOf[DynamoDBQuery[A]], acc)
      case deleteItem @ DeleteItem(_, _, _, _, _, _) =>
        acc.addDeleteItem(deleteItem)
      case putItem @ PutItem(_, _, _, _, _, _)       =>
        acc.addPutItem(putItem)
      case getItem @ GetItem(_, _, _, _, _)          =>
        acc.addGetItem(getItem)
      case atomic                                    =>
        acc + atomic
    }

  // simple aggregation using a List accumulator
  def loop2[A](query: DynamoDBQuery[A], acc: List[DynamoDBQuery[A]]): List[DynamoDBQuery[Any]] =
    query match {
      case Zip(left, right) =>
        loop2(left.asInstanceOf[DynamoDBQuery[A]], acc) ++ loop2(right.asInstanceOf[DynamoDBQuery[A]], acc)
      case q                =>
        acc :+ q
    }

  def aggregate(atomicItems: List[DynamoDBQuery[Any]]): Aggregated =
    atomicItems.foldRight(Aggregated()) {
      case (query, acc) =>
        query match {
          case getItem @ GetItem(_, _, _, _, _) =>
            acc.addGetItem(getItem)
          case q                                => acc + q
        }
    }

  def live =
    ZLayer.fromService[DynamoDb.Service, DynamoDBExecutor.Service](dynamoDb =>
      new Service {
        override def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
          val queries = loop2(query, List.empty)

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

      case BatchGetItem(requestItems, capacity)                                                 =>
        println(s"$requestItems $capacity")
        ZIO.succeed(BatchGetItem.Response(ScalaMap.empty, ScalaMap.empty))

      case BatchWriteItem(requestItems, capacity, metrics)                                      =>
        println(s"$requestItems $capacity $metrics")
        ZIO.succeed(BatchWriteItem.Response(ScalaMap.empty, null))

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

  val queries: List[DynamoDBQuery[Any]] = DynamoDBExecutor.loop2(mixed, List.empty)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val x: ZIO[Any, Exception, List[Any]] = ZIO.foreachPar(queries)(DynamoDBExecutor.executeQueryAgainstDdb)
    x.map(_.foreach(x => println(s"x=$x"))).exitCode
  }
}
