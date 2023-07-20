package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

@implicitNotFound("DynamoDB does not support primary key type ${A}")
sealed trait IsPrimaryKey[A]

// Allowed types for partition and sort keys are: String, Number, Binary
object IsPrimaryKey {
  implicit val intIsPrimaryKey = new IsPrimaryKey[Int] {}
  // TODO: Avi - support other numeric types

  implicit val stringIsPrimaryKey = new IsPrimaryKey[String] {}

  // binary data
  implicit val binaryIsPrimaryKey  = new IsPrimaryKey[Iterable[Byte]] {}
  implicit val binaryIsPrimaryKey2 = new IsPrimaryKey[List[Byte]] {}
  implicit val binaryIsPrimaryKey3 = new IsPrimaryKey[Vector[Byte]] {}
  // TODO: Avi - other collection types
}
