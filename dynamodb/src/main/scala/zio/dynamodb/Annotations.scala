package zio.dynamodb

object Annotations {
  final case class discriminator(name: String) extends scala.annotation.Annotation
  final case class enumOfCaseObjects()         extends scala.annotation.Annotation
  final case class id(value: String)           extends scala.annotation.Annotation

  // TODO: move maybeId here
}
