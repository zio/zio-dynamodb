package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, FromAttributeValue, Item, ToAttributeValue }
import zio.schema.Schema.Primitive
import zio.schema.{ Schema, StandardType }

import scala.annotation.tailrec

object ItemDecoder {

  type Decoder[+A] = AttributeValue => Either[String, A]

  def fromItem[A](item: Item)(implicit schema: Schema[A]): Either[String, A] = {
    val av = ToAttributeValue.attrMapToAttributeValue.toAttributeValue(item)
    decoder(schema)(av)
  }

  @tailrec
  def decoder[A](schema: Schema[A]): Decoder[A] =
    schema match {
      case ProductDecoder(decoder) => decoder // check AV is an AVMap
      case Primitive(standardType) => primitiveDecoder(standardType)
      case l @ Schema.Lazy(_)      => decoder(l.schema)
      case _                       =>
        throw new UnsupportedOperationException(s"schema $schema not yet supported")
    }

  object ProductDecoder {
    def unapply[A](schema: Schema[A]): Option[Decoder[A]] =
      schema match {
        case s @ Schema.CaseClass1(_, _, _, _)             =>
          Some(caseClass1Decoder(s))
        case s @ Schema.CaseClass2(_, _, _, _, _, _)       =>
          Some(caseClass2Decoder(s))
        case s @ Schema.CaseClass3(_, _, _, _, _, _, _, _) =>
          Some(caseClass3Decoder(s))
        case _                                             =>
          None
      }
  }

  private def caseClass1Decoder[A1, Z](schema: Schema.CaseClass1[A1, Z]): Decoder[Z] = { (av: AttributeValue) =>
    decodeFields(av, schema.field).map { xs =>
      schema.construct(xs(0).asInstanceOf[A1])
    }
  }

  private def caseClass2Decoder[A1, A2, Z](schema: Schema.CaseClass2[A1, A2, Z]): Decoder[Z] = { (av: AttributeValue) =>
    decodeFields(av, schema.field1, schema.field2).map { xs =>
      schema.construct(xs(0).asInstanceOf[A1], xs(1).asInstanceOf[A2])
    }
  }

  private def caseClass3Decoder[A1, A2, A3, Z](schema: Schema.CaseClass3[A1, A2, A3, Z]): Decoder[Z] = {
    (av: AttributeValue) =>
      decodeFields(av, schema.field1, schema.field2, schema.field3).map { xs =>
        schema.construct(xs(0).asInstanceOf[A1], xs(1).asInstanceOf[A2], xs(2).asInstanceOf[A3])
      }
  }

  def decodeFields(av: AttributeValue, fields: Schema.Field[_]*): Either[String, List[Any]] =
    av match {
      case AttributeValue.Map(map) =>
        val x: Either[String, Iterable[Any]] = foreach(fields) {
          case Schema.Field(key, schema, _) =>
            val dec        = decoder(schema)
            val maybeValue = map.get(AttributeValue.String(key))
            maybeValue.map(dec).toRight(s"field '$key' not found in $av").flatten
        }

        x.map(_.toList)
      case _                       => Left(s"$av is not an AttributeValue.Map")
    }

  private def foreach[A, B](list: Iterable[A])(f: A => Either[String, B]): Either[String, Iterable[B]] = {
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

  def primitiveDecoder[A](standardType: StandardType[A]): Decoder[A] = { (av: AttributeValue) =>
    standardType match {
      case StandardType.BoolType   =>
        FromAttributeValue[Boolean]
          .asInstanceOf[FromAttributeValue[A]]
          .fromAttributeValue(av)
          .toRight("error getting boolean")
      case StandardType.StringType =>
        FromAttributeValue[String]
          .asInstanceOf[FromAttributeValue[A]]
          .fromAttributeValue(av)
          .toRight("error getting string")
      case StandardType.IntType    =>
        FromAttributeValue[Int].asInstanceOf[FromAttributeValue[A]].fromAttributeValue(av).toRight("error getting Int")
      case _                       => throw new UnsupportedOperationException(s"standardType $standardType not yet supported")

    }
  }

}
