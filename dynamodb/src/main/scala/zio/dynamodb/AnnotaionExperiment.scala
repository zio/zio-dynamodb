package zio.dynamodb

object AnnotaionExperiment {
  final case class discriminator(name: String) extends scala.annotation.Annotation
  final case class constantValue()             extends scala.annotation.Annotation

//  final case class discriminator2(sumTypePolicy: SumTypePolicy) extends scala.annotation.Annotation
//  final class sumTypePolicy2                                    extends scala.annotation.Annotation
//  sealed trait SumTypePolicy
//  case object SumTypePolicyMap                                  extends SumTypePolicy
//  final case class SumTypePolicyDiscriminator(name: String)     extends SumTypePolicy
}
