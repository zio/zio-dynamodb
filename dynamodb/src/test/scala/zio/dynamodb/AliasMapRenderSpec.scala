package zio.dynamodb

import zio.Chunk
import zio.dynamodb.KeyConditionExpr
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.ProjectionExpression._
import zio.test.Assertion._
import zio.test.{ ZIOSpecDefault, _ }

object AliasMapRenderSpec extends ZIOSpecDefault {

  val one    = AttributeValue.Number(1)
  val two    = AttributeValue.Number(2)
  val three  = AttributeValue.Number(3)
  val number = AttributeValue.String("N")
  val name   = AttributeValue.String("name")
  val list   = AttributeValue.List(List(one, two, three))
  val bool   = AttributeValue.String("BOOL")

  val root: ProjectionExpression[Any, Any]                                    = ProjectionExpression.Root
  def rootPathSegment(path: String)                                           = pathSegment(root, path)
  def pathSegment[From, To](pe: ProjectionExpression[From, To], path: String) = AliasMap.PathSegment(pe, path)
  def fullPath[From, To](pe: ProjectionExpression[From, To])                  = AliasMap.FullPath(pe)
  def avKey(av: AttributeValue): AliasMap.Key                                 = AliasMap.AttributeValueKey(av)
  def mapOfAv(av: AttributeValue, s: String): Map[AliasMap.Key, String]       = Map(avKey(av) -> s)

  val between                                                          = ConditionExpression
    .Between(
      ConditionExpression.Operand.ValueOperand(two),
      one,
      three
    )
  val in                                                               = ConditionExpression
    .In(
      ConditionExpression.Operand.ValueOperand(one),
      Set(one, two)
    )
  private val projectionExpression: ProjectionExpression[Any, Unknown] = $("projection")
  val attributeExists                                                  = ConditionExpression.AttributeExists(projectionExpression)
  val attributeNotExists                                               = ConditionExpression.AttributeNotExists(projectionExpression)
  val attributeType                                                    = ConditionExpression
    .AttributeType(projectionExpression, AttributeValueType.Number)
  val contains                                                         = ConditionExpression
    .Contains(
      projectionExpression,
      one
    )
  val beginsWith                                                       = ConditionExpression
    .BeginsWith(
      projectionExpression,
      name
    )

  val setOperandValueOne   = UpdateExpression.SetOperand.ValueOperand[Any](one)
  val setOperandValueTwo   = UpdateExpression.SetOperand.ValueOperand[Any](two)
  val setOperandValueThree = UpdateExpression.SetOperand.ValueOperand[Any](three)

  override def spec: Spec[_root_.zio.test.TestEnvironment, Any] = main

  val main: Spec[_root_.zio.test.TestEnvironment, Any] =
    suite("AliasMapRender")(
      suite("ConditionExpression")(
        suite("basic renders")(
          test("Between") {
            val (aliasMap, expression) = between.render.execute
            val map                    = Map(
              avKey(two)   -> ":v0",
              avKey(one)   -> ":v1",
              avKey(three) -> ":v2"
            )

            assert(aliasMap)(equalTo(AliasMap(map, 3))) &&
            assert(expression)(equalTo(":v0 BETWEEN :v1 AND :v2"))
          },
          test("In") {
            val (aliasMap, expression) = in.render.execute
            val map                    = Map(
              avKey(one) -> ":v0",
              avKey(two) -> ":v1"
            )

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(":v0 IN (:v0, :v1)"))
          },
          test("AttributeExists") {
            val map = Map(
              rootPathSegment("projection")  -> "#n0",
              fullPath(projectionExpression) -> "#n0"
            )

            val (aliasMap, expression) =
              attributeExists.render.execute
            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo(s"attribute_exists(#n0)"))
          },
          test("AttributeNotExists") {
            val map = Map(
              rootPathSegment("projection")  -> "#n0",
              fullPath(projectionExpression) -> "#n0"
            )

            val (aliasMap, expression) =
              attributeNotExists.render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo(s"attribute_not_exists(#n0)"))
          },
          test("AttributeType") {
            val map = Map(
              avKey(number)                  -> ":v0",
              rootPathSegment("projection")  -> "#n1",
              fullPath(projectionExpression) -> "#n1"
            )

            val (aliasMap, expression) = attributeType.render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"attribute_type(#n1, :v0)"))
          },
          test("Contains") {
            val map = Map(
              avKey(one)                     -> ":v0",
              rootPathSegment("projection")  -> "#n1",
              fullPath(projectionExpression) -> "#n1"
            )

            val (aliasMap, expression) = contains.render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"contains(#n1, :v0)"))
          },
          test("BeginsWith") {
            val map = Map(
              avKey(name)                    -> ":v0",
              rootPathSegment("projection")  -> "#n1",
              fullPath(projectionExpression) -> "#n1"
            )

            val (aliasMap, expression) = beginsWith.render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"begins_with(#n1, :v0)"))
          },
          test("And") {
            val map = Map(
              avKey(two)                     -> ":v0",
              avKey(one)                     -> ":v1",
              avKey(three)                   -> ":v2",
              avKey(number)                  -> ":v3",
              rootPathSegment("projection")  -> "#n4",
              fullPath(projectionExpression) -> "#n4"
            )

            val (aliasMap, expression) = ConditionExpression
              .And(
                between,
                attributeType
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 5))) &&
            assert(expression)(equalTo(s"(:v0 BETWEEN :v1 AND :v2) AND (attribute_type(#n4, :v3))"))
          },
          test("Or") {
            val map = Map(
              avKey(two)                     -> ":v0",
              avKey(one)                     -> ":v1",
              avKey(three)                   -> ":v2",
              avKey(number)                  -> ":v3",
              rootPathSegment("projection")  -> "#n4",
              fullPath(projectionExpression) -> "#n4"
            )

            val (aliasMap, expression) = ConditionExpression
              .Or(
                between,
                attributeType
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 5))) &&
            assert(expression)(equalTo(s"(:v0 BETWEEN :v1 AND :v2) OR (attribute_type(#n4, :v3))"))
          },
          test("Not") {
            val map = Map(
              avKey(name)                    -> ":v0",
              rootPathSegment("projection")  -> "#n1",
              fullPath(projectionExpression) -> "#n1"
            )

            val (aliasMap, expression) = ConditionExpression
              .Not(beginsWith)
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"NOT (begins_with(#n1, :v0))"))
          },
          test("Equals") {
            val map = Map(
              avKey(two)                     -> ":v0",
              rootPathSegment("projection")  -> "#n1",
              fullPath(projectionExpression) -> "#n1"
            )

            val (aliasMap, expression) = ConditionExpression
              .Equals(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.Size(projectionExpression, null)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"(:v0) = (size(#n1))"))
          },
          test("Equals with duplicates") {
            val map = Map(
              avKey(two) -> ":v0"
            )

            val (aliasMap, expression) = ConditionExpression
              .Equals(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.ValueOperand(two)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo(s"(:v0) = (:v0)"))
          },
          test("NotEqual") {
            val map = Map(
              avKey(two)                     -> ":v0",
              rootPathSegment("projection")  -> "#n1",
              fullPath(projectionExpression) -> "#n1"
            )

            val (aliasMap, expression) = ConditionExpression
              .NotEqual(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.Size(projectionExpression, null)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"(:v0) <> (size(#n1))"))
          },
          test("LessThan") {
            val map = Map(
              avKey(one)   -> ":v0",
              avKey(three) -> ":v1"
            )

            val (aliasMap, expression) = ConditionExpression
              .LessThan(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo("(:v0) < (:v1)"))
          },
          test("GreaterThan") {
            val map = Map(
              avKey(one)   -> ":v0",
              avKey(three) -> ":v1"
            )

            val (aliasMap, expression) = ConditionExpression
              .GreaterThan(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo("(:v0) > (:v1)"))
          },
          test("LessThanOrEqual") {
            val map = Map(
              avKey(one)   -> ":v0",
              avKey(three) -> ":v1"
            )

            val (aliasMap, expression) = ConditionExpression
              .LessThanOrEqual(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo("(:v0) <= (:v1)"))
          },
          test("GreaterThanOrEqual") {
            val map = Map(
              avKey(one)   -> ":v0",
              avKey(three) -> ":v1"
            )

            val (aliasMap, expression) = ConditionExpression
              .GreaterThanOrEqual(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo("(:v0) >= (:v1)"))
          }
        )
      ),
      // TODO: Avi - create new tests suite just for KeyConditionExpr ???
      suite("KeyConditionExpr")(
        suite("Sort key expressions")(
          test("Equals") {
            val map = Map(
              avKey(one) -> ":v0"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.SortKeyEquals($("num").sortKey, one).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("num = :v0"))
          },
          test("LessThan") {
            val map = Map(
              avKey(one) -> ":v0"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.ExtendedSortKeyExpr.LessThan($("num").sortKey, one).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("num < :v0"))
          },
          test("NotEqual") {
            val map = Map(
              avKey(one) -> ":v0"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.ExtendedSortKeyExpr.NotEqual($("num").sortKey, one).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("num <> :v0"))
          },
          test("GreaterThan") {
            val map = Map(
              avKey(one) -> ":v0"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.ExtendedSortKeyExpr.GreaterThan($("num").sortKey, one).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("num > :v0"))
          },
          test("LessThanOrEqual") {
            val map = Map(
              avKey(one) -> ":v0"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.ExtendedSortKeyExpr.LessThanOrEqual($("num").sortKey, one).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("num <= :v0"))
          },
          test("GreaterThanOrEqual") {
            val map = Map(
              avKey(one) -> ":v0"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.ExtendedSortKeyExpr.GreaterThanOrEqual($("num").sortKey, one).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("num >= :v0"))
          },
          test("Between") {
            val map = Map(
              avKey(one) -> ":v0",
              avKey(two) -> ":v1"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.ExtendedSortKeyExpr.Between($("num").sortKey, one, two).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo("num BETWEEN :v0 AND :v1"))
          },
          test("BeginsWith") {
            val map = Map(
              avKey(name) -> ":v0"
            )

            val (aliasMap, expression) =
              KeyConditionExpr.ExtendedSortKeyExpr.BeginsWith($("num").sortKey, name).render2.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("begins_with(num, :v0)"))
          }
        ),
        suite("PartitionKeyExpression")(
          test("Equals") {
            val map = Map(
              avKey(one) -> ":v0"
            )

            val (aliasMap, expression) = KeyConditionExpr
              .PartitionKeyEquals($("num").partitionKey, one)
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo("num = :v0"))
          }
        ),
        test("And") {
          val map = Map(
            avKey(two)   -> ":v0",
            avKey(one)   -> ":v1",
            avKey(three) -> ":v2"
          )

          val (aliasMap, expression) = KeyConditionExpr
            .ExtendedCompositePrimaryKeyExpr(
              KeyConditionExpr.PartitionKeyEquals($("num").partitionKey, two),
              KeyConditionExpr.ExtendedSortKeyExpr.Between($("num").sortKey, one, three)
            )
            .render
            .execute

          assert(aliasMap)(equalTo(AliasMap(map, 3))) &&
          assert(expression)(equalTo("num = :v0 AND num BETWEEN :v1 AND :v2"))
        }
      ),
      suite("AttributeValueType")(
        test("Bool") {
          val map = Map(
            avKey(AttributeValue.String("BOOL")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.Bool.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("BinarySet") {
          val map = Map(
            avKey(AttributeValue.String("BS")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.BinarySet.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("List") {
          val map = Map(
            avKey(AttributeValue.String("L")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.List.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Map") {
          val map = Map(
            avKey(AttributeValue.String("M")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.Map.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("NumberSet") {
          val map = Map(
            avKey(AttributeValue.String("NS")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.NumberSet.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Null") {
          val map = Map(
            avKey(AttributeValue.String("NULL")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.Null.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("StringSet") {
          val map = Map(
            avKey(AttributeValue.String("SS")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.StringSet.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Binary") {
          val map = Map(
            avKey(AttributeValue.String("B")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.Binary.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Number") {
          val map = Map(
            avKey(AttributeValue.String("N")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.Number.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("String") {
          val map = Map(
            avKey(AttributeValue.String("S")) -> ":v0"
          )

          val (aliasMap, expression) = AttributeValueType.String.render.execute

          assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
          assert(expression)(equalTo(":v0"))
        }
      ),
      suite("UpdateExpression")(
        suite("multiple actions")(
          test("Set and Remove") {
            val map = Map(
              rootPathSegment("projection")      -> "#n0",
              fullPath($("projection"))          -> "#n0",
              avKey(one)                         -> ":v1",
              rootPathSegment("otherProjection") -> "#n3",
              fullPath($("otherProjection"))     -> "#n3"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.Actions[Any](
                  Chunk(
                    UpdateExpression.Action.SetAction[Any, Any](
                      $("projection"),
                      UpdateExpression.SetOperand.IfNotExists(
                        projectionExpression,
                        one
                      )
                    ),
                    UpdateExpression.Action.RemoveAction($("otherProjection"))
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 5))) &&
            assert(expression)(equalTo(s"set #n0 = if_not_exists(#n0, :v1) remove #n3"))
          },
          test("Two Sets") {
            val map = Map(
              rootPathSegment("projection")      -> "#n0",
              fullPath($("projection"))          -> "#n0",
              avKey(one)                         -> ":v1",
              rootPathSegment("otherProjection") -> "#n2",
              fullPath($("otherProjection"))     -> "#n2",
              rootPathSegment("lastProjection")  -> "#n5",
              fullPath($("lastProjection"))      -> "#n5"
            )

            val (aliasMap, expression) =
              (UpdateExpression.Action.SetAction($("projection"), UpdateExpression.SetOperand.ValueOperand(one)) +
                UpdateExpression.Action.SetAction(
                  $("otherProjection"),
                  UpdateExpression.SetOperand.ValueOperand(one)
                ) + UpdateExpression.Action.AddAction($("lastProjection"), one)).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 7))) && // TODO: Avi resolve 7
            assert(expression)(equalTo(s"set #n0 = :v1,#n2 = :v1 add #n5 :v1"))
          }
        ),
        suite("Set")(
          test("Minus") {
            val map = Map(
              avKey(one)                    -> ":v0",
              avKey(two)                    -> ":v1",
              rootPathSegment("projection") -> "#n2",
              fullPath($("projection"))     -> "#n2"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $("projection"),
                  UpdateExpression.SetOperand.Minus[Any](
                    setOperandValueOne,
                    setOperandValueTwo
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 3))) &&
            assert(expression)(equalTo(s"set #n2 = :v0 - :v1"))
          },
          test("Plus") {
            val map = Map(
              avKey(one)                    -> ":v0",
              avKey(two)                    -> ":v1",
              rootPathSegment("projection") -> "#n2",
              fullPath($("projection"))     -> "#n2"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $("projection"),
                  UpdateExpression.SetOperand.Plus(
                    setOperandValueOne,
                    setOperandValueTwo
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 3))) &&
            assert(expression)(equalTo(s"set #n2 = :v0 + :v1"))

          },
          test("ValueOperand") {
            val map = Map(
              avKey(one)                    -> ":v0",
              rootPathSegment("projection") -> "#n1",
              fullPath($("projection"))     -> "#n1"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $("projection"),
                  UpdateExpression.SetOperand.ValueOperand(one)
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"set #n1 = :v0"))

          },
          test("PathOperand") {
            val map = Map(
              rootPathSegment("projection") -> "#n0",
              fullPath($("projection"))     -> "#n0"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $("projection"),
                  UpdateExpression.SetOperand.PathOperand(
                    projectionExpression
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo(s"set #n0 = #n0"))

          },
          test("ListAppend") {
            val map = Map(
              rootPathSegment("projection") -> "#n0",
              fullPath($("projection"))     -> "#n0",
              avKey(list)                   -> ":v1"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $("projection"),
                  UpdateExpression.SetOperand.ListAppend(
                    projectionExpression,
                    list
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"set #n0 = list_append(#n0, :v1)"))

          },
          test("ListPrepend") {
            val map = Map(
              rootPathSegment("projection") -> "#n0",
              fullPath($("projection"))     -> "#n0",
              avKey(list)                   -> ":v1"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $("projection"),
                  UpdateExpression.SetOperand.ListPrepend(
                    projectionExpression,
                    list
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"set #n0 = list_append(:v1, #n0)"))

          },
          test("IfNotExists") {
            val map = Map(
              rootPathSegment("projection") -> "#n0",
              fullPath($("projection"))     -> "#n0",
              avKey(one)                    -> ":v1"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $("projection"),
                  UpdateExpression.SetOperand.IfNotExists(
                    projectionExpression,
                    one
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"set #n0 = if_not_exists(#n0, :v1)"))

          }
        ),
        suite("Remove")(
          test("Remove") {
            val map = Map(
              rootPathSegment("projection") -> "#n0",
              fullPath($("projection"))     -> "#n0"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.RemoveAction(
                  $("projection")
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 1))) &&
            assert(expression)(equalTo(s"remove #n0"))
          }
        ),
        suite("Add")(
          test("Add") {
            val map = Map(
              rootPathSegment("projection") -> "#n0",
              fullPath($("projection"))     -> "#n0",
              avKey(one)                    -> ":v1"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.AddAction(
                  $("projection"),
                  one
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"add #n0 :v1"))
          }
        ),
        suite("Delete")(
          test("Delete") {
            val map = Map(
              rootPathSegment("projection") -> "#n0",
              fullPath($("projection"))     -> "#n0",
              avKey(one)                    -> ":v1"
            )

            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.DeleteAction(
                  $("projection"),
                  one
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(map, 2))) &&
            assert(expression)(equalTo(s"delete #n0 :v1"))
          }
        )
      )
    )

}
