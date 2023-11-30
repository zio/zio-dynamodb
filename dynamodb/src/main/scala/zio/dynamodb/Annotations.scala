package zio.dynamodb

import zio.Chunk
import zio.schema.annotation.{ caseName, discriminatorName }
import scala.annotation.StaticAnnotation
import zio.schema.annotation.noDiscriminator
import zio.schema.annotation.simpleEnum

object Annotations {
  final case class simpleEnumField() extends StaticAnnotation

  def maybeCaseName(annotations: Chunk[Any]): Option[String]      =
    annotations.collect { case caseName(name) => name }.headOption

  def maybeDiscriminator(annotations: Chunk[Any]): Option[String] =
    annotations.collect { case discriminatorName(name) => name }.headOption

  def hasNoDiscriminator(annotations: Chunk[Any]): Boolean = {
    val collected = annotations.collect { case noDiscriminator() => noDiscriminator }
    collected.headOption.isDefined
  }

  def hasSimpleEnum(annotations: Chunk[Any]): Boolean = {
    val collected = annotations.collect { case simpleEnum(_) => simpleEnum() }
    collected.headOption.isDefined
  }

}
