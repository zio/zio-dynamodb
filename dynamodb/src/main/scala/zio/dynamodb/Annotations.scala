package zio.dynamodb

import zio.Chunk

object Annotations {
  final case class discriminator(name: String) extends scala.annotation.Annotation
  final case class enumOfCaseObjects()         extends scala.annotation.Annotation
  final case class id(value: String)           extends scala.annotation.Annotation

  def maybeId(annotations: Chunk[Any]): Option[String]            =
    annotations.collect { case id(name) => name }.headOption

  def maybeDiscriminator(annotations: Chunk[Any]): Option[String] =
    annotations.collect { case discriminator(name) => name }.headOption
}
