package zio.dynamodb

import zio.Chunk
import zio.schema.annotation.{ caseName }

object Annotations {
  final case class discriminator(name: String) extends scala.annotation.Annotation
  final case class enumOfCaseObjects()         extends scala.annotation.Annotation

  def maybeCaseName(annotations: Chunk[Any]): Option[String]      =
    annotations.collect { case caseName(name) => name }.headOption

  def maybeDiscriminator(annotations: Chunk[Any]): Option[String] =
    annotations.collect { case discriminator(name) => name }.headOption
}
