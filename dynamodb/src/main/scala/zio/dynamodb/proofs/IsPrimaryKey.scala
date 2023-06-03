package zio.dynamodb.proofs

sealed trait IsPrimaryKey[A]

object IsPrimaryKey {
  implicit val intIsPrimaryKey    = new IsPrimaryKey[Int] {}
  implicit val stringIsPrimaryKey = new IsPrimaryKey[String] {}
  // todo binary data
}
