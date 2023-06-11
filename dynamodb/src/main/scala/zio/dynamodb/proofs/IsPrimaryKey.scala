package zio.dynamodb.proofs

sealed trait IsPrimaryKey[A]

object IsPrimaryKey {
  implicit val intIsPrimaryKey = new IsPrimaryKey[Int] {}

  // HOWEVER - String type in the DB is overloaded in Scala land eg Date in Scala is String in DB
  // so do we also allow Scala types that are strings in the DB?
  implicit val stringIsPrimaryKey = new IsPrimaryKey[String] {}
  // todo binary data
}
