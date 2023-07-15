package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

@implicitNotFound("DynamoDB does not support primary key type ${A}")
sealed trait IsPrimaryKey[A]

// Allowed types for partition and sort keys are: String, Number, Binary
object IsPrimaryKey {
  implicit val intIsPrimaryKey = new IsPrimaryKey[Int] {}

  // TODO - String type in the DB is overloaded in Scala land eg Date in Scala is String in DB
  // so do we also allow Scala types that are strings in the DB?
  implicit val stringIsPrimaryKey = new IsPrimaryKey[String] {}

  // binary data
  implicit val binaryIsPrimaryKey = new IsPrimaryKey[Iterable[Byte]] {}
  implicit val binaryIsPrimaryKey2 = new IsPrimaryKey[List[Byte]] {}
}
