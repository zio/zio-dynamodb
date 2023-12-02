package zio.dynamodb

import zio.Chunk
import zio.schema.annotation.{ caseName, discriminatorName }
import zio.schema.annotation.noDiscriminator
import zio.schema.annotation.simpleEnum

private[dynamodb] object Annotations {

  def discriminatorWithDefault(annotations: Chunk[Any]): String =
    maybeDiscriminator(annotations).getOrElse("discriminator")

  def hasAnnotationAtClassLevel(annotations: Chunk[Any]): Boolean =
    annotations.exists {
      case discriminatorName(_) | simpleEnum(_) | noDiscriminator() => true
      case _                                                        => false
    }

  def hasNoDiscriminator(annotations: Chunk[Any]): Boolean =
    annotations.exists {
      case noDiscriminator() => true
      case _                 => false
    }

  def hasSimpleEnum(annotations: Chunk[Any]): Boolean =
    annotations.exists {
      case simpleEnum(true) => true
      case _                => false
    }

  def maybeCaseName(annotations: Chunk[Any]): Option[String]      =
    annotations.collect { case caseName(name) => name }.headOption

  def maybeDiscriminator(annotations: Chunk[Any]): Option[String] =
    annotations.collect { case discriminatorName(name) => name }.headOption

}
