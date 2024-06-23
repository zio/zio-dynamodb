package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

@implicitNotFound(
  "DynamoDB does not support primary key type ${A} - allowed types are: String, Number, Binary and Options of those types"
)
sealed trait IsPrimaryKey[-A]

object IsPrimaryKey {
  implicit val stringIsPrimaryKey: IsPrimaryKey[String] = new IsPrimaryKey[String] {}

  implicit val shortIsPrimaryKey: IsPrimaryKey[Short]           = new IsPrimaryKey[Short] {}
  implicit val intIsPrimaryKey: IsPrimaryKey[Int]               = new IsPrimaryKey[Int] {}
  implicit val longIsPrimaryKey: IsPrimaryKey[Long]             = new IsPrimaryKey[Long] {}
  implicit val floatIsPrimaryKey: IsPrimaryKey[Float]           = new IsPrimaryKey[Float] {}
  implicit val doubleIsPrimaryKey: IsPrimaryKey[Double]         = new IsPrimaryKey[Double] {}
  implicit val bigDecimalIsPrimaryKey: IsPrimaryKey[BigDecimal] = new IsPrimaryKey[BigDecimal] {}
  implicit val binaryIsPrimaryKey: IsPrimaryKey[Iterable[Byte]] = new IsPrimaryKey[Iterable[Byte]] {}

  implicit val stringIsPrimaryKeyOpt: IsPrimaryKey[Option[String]]         = new IsPrimaryKey[Option[String]] {}
  implicit val shortIsPrimaryKeyOpt: IsPrimaryKey[Option[Short]]           = new IsPrimaryKey[Option[Short]] {}
  implicit val intIsPrimaryKeyOpt: IsPrimaryKey[Option[Int]]               = new IsPrimaryKey[Option[Int]] {}
  implicit val longIsPrimaryKeyOpt: IsPrimaryKey[Option[Long]]             = new IsPrimaryKey[Option[Long]] {}
  implicit val floatIsPrimaryKeyOpt: IsPrimaryKey[Option[Float]]           = new IsPrimaryKey[Option[Float]] {}
  implicit val doubleIsPrimaryKeyOpt: IsPrimaryKey[Option[Double]]         = new IsPrimaryKey[Option[Double]] {}
  implicit val bigDecimalIsPrimaryKeyOpt: IsPrimaryKey[Option[BigDecimal]] =
    new IsPrimaryKey[Option[BigDecimal]] {}
  implicit val binaryIsPrimaryKeyOpt: IsPrimaryKey[Option[Iterable[Byte]]] =
    new IsPrimaryKey[Option[Iterable[Byte]]] {}

}
