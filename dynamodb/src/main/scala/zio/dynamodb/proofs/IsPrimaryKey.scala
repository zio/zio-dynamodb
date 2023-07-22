package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

@implicitNotFound("DynamoDB does not support primary key type ${A} - allowed types are: String, Number, Binary")
sealed trait IsPrimaryKey[A]

object IsPrimaryKey {
  implicit val intIsPrimaryKey: IsPrimaryKey[Int] = new IsPrimaryKey[Int] {}
  // TODO: Avi - support other numeric types

  implicit val stringIsPrimaryKey: IsPrimaryKey[String] = new IsPrimaryKey[String] {}

  // binary data
  implicit val binaryIsPrimaryKey: IsPrimaryKey[Iterable[Byte]] = new IsPrimaryKey[Iterable[Byte]] {}
  implicit val binaryIsPrimaryKey2: IsPrimaryKey[List[Byte]]    = new IsPrimaryKey[List[Byte]] {}
  implicit val binaryIsPrimaryKey3: IsPrimaryKey[Vector[Byte]]  = new IsPrimaryKey[Vector[Byte]] {}
  // TODO: Avi - other collection types
}
