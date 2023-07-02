package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

@implicitNotFound("DynamoDB does not support primary key type ${A}")
sealed trait IsPrimaryKey[A]

object IsPrimaryKey {
  implicit val intIsPrimaryKey = new IsPrimaryKey[Int] {}

  // TODO - String type in the DB is overloaded in Scala land eg Date in Scala is String in DB
  // so do we also allow Scala types that are strings in the DB?
  implicit val stringIsPrimaryKey = new IsPrimaryKey[String] {}
  // todo binary data
}
