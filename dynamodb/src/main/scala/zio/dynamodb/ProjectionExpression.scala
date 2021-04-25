package zio.dynamodb

import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, PathOperand }

// The maximum depth for a document path is 32
sealed trait ProjectionExpression { self =>
  def apply(index: Int): ProjectionExpression = ProjectionExpression.ListElement(self, index)

  def apply(key: String): ProjectionExpression = ProjectionExpression.MapElement(self, key)

  def exists: ConditionExpression                                    = ConditionExpression.AttributeExists(self)
  def notExists: ConditionExpression                                 = ConditionExpression.AttributeNotExists(self)
  def contains(av: AttributeValue): ConditionExpression              = ConditionExpression.Contains(self, av)
  def beginsWith(av: AttributeValue): ConditionExpression            = ConditionExpression.BeginsWith(self, av)
  def isType(attributeType: AttributeValueType): ConditionExpression =
    ConditionExpression.AttributeType(self, attributeType)

  def size: ConditionExpression.Operand.Size = ConditionExpression.Operand.Size(self)

  def set[A](a: A)(implicit t: ToAttributeValue[A]): UpdateExpression.Action.SetAction                     =
    UpdateExpression.Action.SetAction(self, UpdateExpression.SetOperand.ValueOperand(t.toAttributeValue(a)))
  def set(pe: ProjectionExpression): UpdateExpression.Action.SetAction                                     =
    UpdateExpression.Action.SetAction(self, PathOperand(pe))
  // TODO: propogate type classes with type parameters
  def setIfNotExists(pe: ProjectionExpression, av: AttributeValue): UpdateExpression.Action.SetAction      =
    UpdateExpression.Action.SetAction(self, IfNotExists(pe, av))
  def setListAppend(xs1: AttributeValue.List, xs2: AttributeValue.List): UpdateExpression.Action.SetAction =
    UpdateExpression.Action.SetAction(self, ListAppend(xs1, xs2))
  def add(av: AttributeValue): UpdateExpression.Action.AddAction                                           =
    UpdateExpression.Action.AddAction(self, av)
  def remove: UpdateExpression.Action.RemoveAction                                                         =
    UpdateExpression.Action.RemoveAction(self)
  def delete(av: AttributeValue): UpdateExpression.Action.DeleteAction                                     =
    UpdateExpression.Action.DeleteAction(self, av)

}

object ProjectionExpression {
  // Note that you can only use a ProjectionExpression if the first character is a-z or A-Z and the second character
  // (if present) is a-z, A-Z, or 0-9. Also key words are not allowed
  // If this is not the case then you must use the Expression Attribute Names facility to create an alias.
  // Attribute names containing a dot "." must also use the Expression Attribute Names
  def apply(name: String): ProjectionExpression = TopLevel(name)

  final case class TopLevel(name: String)                                extends ProjectionExpression
  final case class MapElement(parent: ProjectionExpression, key: String) extends ProjectionExpression
  // index must be non negative - we could use a new type here?
  final case class ListElement(parent: ProjectionExpression, index: Int) extends ProjectionExpression
}
