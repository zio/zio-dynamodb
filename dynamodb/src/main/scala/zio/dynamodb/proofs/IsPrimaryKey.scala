package zio.dynamodb.proofs

import scala.annotation.implicitNotFound
import zio.dynamodb.KeyConditionExpr

@implicitNotFound("DynamoDB does not support primary key type ${A} - allowed types are: String, Number, Binary")
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

  implicit val sortKeyNotUsed: IsPrimaryKey[KeyConditionExpr.SortKeyNotUsed] =
    new IsPrimaryKey[KeyConditionExpr.SortKeyNotUsed] {}
}
