//package com.disneystreaming.subscription.v2.infra.dynamo.formats
//
//import cats.Apply
//import dynosaur.Schema
//import cats.implicits._
//import shapeless.HList
//
//import java.time.Instant
//import java.time.temporal.ChronoUnit
//
//trait DynamoBaseSchema {
//
//  private[formats] def enumSchema(e: Enumeration): Schema[e.Value] =
//    Schema[String].imapErr(str =>
//      e.values
//        .find(_.toString == str)
//        .toRight(
//          Schema.ReadError(
//            s"Unidentified ${e.toString}: $str. Accepted values: ${e.values.mkString(",")}"
//          )
//        )
//    )(_.toString)
//
//  private[formats] implicit lazy val chronoUnitSchema: Schema[ChronoUnit] =
//    Schema[String]
//      .imapErr(s =>
//        Either
//          .catchNonFatal(
//            ChronoUnit.valueOf(s.toUpperCase)
//          )
//          .leftMap(e => Schema.ReadError(e.getMessage))
//      )(_.name())
//
//  private[dynamo] implicit lazy val instantSchema: Schema[Instant] =
//    Schema[String].imapErr(s =>
//      Either
//        .catchNonFatal(Instant.parse(s))
//        .leftMap(e => Schema.ReadError(s"Invalid instant $s: ${e.getMessage}"))
//    )(_.toString)
//
//}
//
//private[formats] object ShapelessSchemas {
//
//  implicit final class FieldOfExt[A <: HList, F[_]: Apply](private val fa: F[A]) {
//
//    import shapeless.::
//
//    // Concatenates values inside two applicatives into a single HList
//    // e.g. (a: IO[Int]) :: (b: IO[String :: HNil]) => IO[Int :: String :: HNil]
//    def ::[B](fb: F[B]): F[B :: A] = (fb, fa).mapN((e, a) => e :: a)
//  }
//
//}
