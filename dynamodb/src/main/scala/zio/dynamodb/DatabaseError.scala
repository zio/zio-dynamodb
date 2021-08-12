package zio.dynamodb

sealed trait DatabaseError                             extends Exception
final case class TableDoesNotExists(tableName: String) extends DatabaseError
