package zio.dynamodb

sealed trait DatabaseError extends Exception

object DatabaseError {
  final case class TableDoesNotExists(tableName: String) extends DatabaseError
}
