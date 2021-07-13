package zio.dynamodb

import scala.annotation.tailrec

final case class AttrMap(map: Map[String, AttributeValue]) {

  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[String, A] =
    map.get(field).flatMap(ev.fromAttributeValue).toRight(s"field '$field' not found")

  def getOpt[A](field: String)(implicit ev: FromAttributeValue[A]): Either[Nothing, Option[A]] =
    Right(map.get(field).flatMap(ev.fromAttributeValue))

  // convenience method so that user does not have to transform between an Option and an Either
  def getOptionalItem[A](
    field: String
  )(f: AttrMap => Either[String, Option[A]]): Either[String, Option[A]] =
    getOpt[Item](field).flatMap(_.fold[Either[String, Option[A]]](Right(None))(f))

  // convenience method so that user does not have to transform between a List and an Either
  def getIterableItem[A](field: String)(f: AttrMap => Either[String, A]): Either[String, Iterable[A]] =
    get[Iterable[Item]](field).flatMap[String, Iterable[A]](xs => traverse(xs.toList)(f))

  // convenience method so that user does not have to transform between an Option, List and an Either
  def getOptionalIterableItem[A](
    field: String
  )(f: AttrMap => Either[String, A]): Either[String, Option[Iterable[A]]] = {
    def maybeTransform(maybeItems: Option[Iterable[Item]]): Either[String, Option[Iterable[A]]] =
      maybeItems match {
        case None     => Right(None)
        case Some(xs) => traverse(xs.toList)(f).map(Some(_))
      }
    getOpt[Iterable[Item]](field: String).flatMap(maybeTransform)
  }

  private def traverse[A, B](list: Iterable[A])(f: A => Either[String, B]): Either[String, Iterable[B]] = {
    @tailrec
    def loop[A2, B2](xs: Iterable[A2], acc: List[B2])(f: A2 => Either[String, B2]): Either[String, Iterable[B2]] =
      xs match {
        case head :: tail =>
          f(head) match {
            case Left(e)  => Left(e)
            case Right(a) => loop(tail, a :: acc)(f)
          }
        case Nil          => Right(acc.reverse)
      }

    loop(list.toList, List.empty)(f)
  }
}

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
