/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbExprInterpreter }
import zio.dynamodb.blocks.ddbexpr.DdbExpr.{ OpticDdbExprOps, OpticUpdateOps }
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.test._
import zio.test.Assertion._

// Models lifted to object scope: Scala 3's initialization model creates a
// circular init cycle when a sealed trait and a companion that references it
// via optic macro (when[Case]) are defined inside the same suite block.
private sealed trait DrawingShape2
private object DrawingShape2 {
  final case class Circle(radius: Double) extends DrawingShape2
  case object Square                      extends DrawingShape2
  implicit val schema: Schema[DrawingShape2] = Schema.derived[DrawingShape2]
}
private case class Drawing2(name: String, shape: DrawingShape2, coords: List[Int])
private object Drawing2 extends CompanionOptics[Drawing2] {
  implicit val schema: Schema[Drawing2]        = Schema.derived
  val name: Lens[Drawing2, String]             = $(_.name)
  val shape: Lens[Drawing2, DrawingShape2]     = $(_.shape)
  val circleRadius: Optional[Drawing2, Double] = $(_.shape.when[DrawingShape2.Circle].radius)
  def coordAt(i: Int): Optional[Drawing2, Int] = $(_.coords.at(i))
}

object DdbExprOptionalSpec extends ZIOSpecDefault {

  private def renderCE(ce: ConditionExpression[_]): String             = ce.render.execute._2
  private def renderAction(action: UpdateExpression.Action[_]): String = action.render.execute._2

  private def interpret[S](expr: DdbExpr[S, Boolean]): Either[String, ConditionExpression[S]] =
    DdbExprInterpreter.toConditionExpression(expr)

  def spec = suite("DdbExpr — Optional optic paths")(
    suite("Optional via when[Case].field")(
      test("attributeExists on a variant-case Optional renders attribute_exists") {
        val expr = Drawing2.circleRadius.attributeExists
        interpret(expr)
          .map(renderCE)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("attribute_exists"))
          )
      },
      test("attributeNotExists on a variant-case Optional renders attribute_not_exists") {
        val expr = Drawing2.circleRadius.attributeNotExists
        interpret(expr)
          .map(renderCE)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("attribute_not_exists"))
          )
      },
      test("set on a variant-case Optional renders SET") {
        val rendered = renderAction(Drawing2.circleRadius.set(2.5))
        assert(rendered)(startsWithString("set") && containsString("="))
      },
      test("remove on a variant-case Optional renders REMOVE") {
        val rendered = renderAction(Drawing2.circleRadius.remove)
        assert(rendered)(startsWithString("remove"))
      }
    ),

    suite("Optional via at(index)")(
      test("attributeExists on an indexed list element renders attribute_exists") {
        val expr = Drawing2.coordAt(0).attributeExists
        interpret(expr)
          .map(renderCE)
          .fold(
            _ => assertNever("interpreter failed"),
            s => assert(s)(startsWithString("attribute_exists"))
          )
      },
      test("remove on an indexed list element renders REMOVE with list index syntax") {
        val rendered = renderAction(Drawing2.coordAt(0).remove)
        assert(rendered)(startsWithString("remove") && containsString("[0]"))
      },
      test("set on an indexed list element renders SET with index in path") {
        val rendered = renderAction(Drawing2.coordAt(2).set(99))
        assert(rendered)(startsWithString("set") && containsString("[2]"))
      }
    ),

    suite("Optional via Map[String, V].atKey") {
      case class Registry(name: String, scores: Map[String, Int])
      object Registry extends CompanionOptics[Registry] {
        implicit val schema: Schema[Registry]             = Schema.derived
        val name: Lens[Registry, String]                  = $(_.name)
        def scoreAt(key: String): Optional[Registry, Int] = $(_.scores.atKey(key))
      }

      suite("map key Optional")(
        test("attributeExists on a map-key Optional renders attribute_exists") {
          val expr = Registry.scoreAt("alice").attributeExists
          interpret(expr)
            .map(renderCE)
            .fold(
              _ => assertNever("interpreter failed"),
              s => assert(s)(startsWithString("attribute_exists"))
            )
        },
        test("attributeNotExists on a map-key Optional renders attribute_not_exists") {
          val expr = Registry.scoreAt("alice").attributeNotExists
          interpret(expr)
            .map(renderCE)
            .fold(
              _ => assertNever("interpreter failed"),
              s => assert(s)(startsWithString("attribute_not_exists"))
            )
        },
        test("set on a map-key Optional renders SET") {
          val rendered = renderAction(Registry.scoreAt("alice").set(99))
          assert(rendered)(startsWithString("set") && containsString("="))
        },
        test("remove on a map-key Optional renders REMOVE") {
          val rendered = renderAction(Registry.scoreAt("alice").remove)
          assert(rendered)(startsWithString("remove"))
        }
      )
    },

    suite("Option[A] transparency — Case(Some)+Field(value) pruned") {
      case class Profile(name: String, tags: Option[List[String]], meta: Option[Map[String, Int]])
      object Profile extends CompanionOptics[Profile] {
        implicit val schema: Schema[Profile]            = Schema.derived
        val name: Lens[Profile, String]                 = $(_.name)
        def tagAt(i: Int): Optional[Profile, String]    = $(_.tags.when[Some[List[String]]].value.at(i))
        def metaAt(key: String): Optional[Profile, Int] = $(_.meta.when[Some[Map[String, Int]]].value.atKey(key))
      }

      suite("Option[A] paths")(
        test("attributeExists on Option[List] element renders attribute_exists") {
          val expr = Profile.tagAt(0).attributeExists
          interpret(expr)
            .map(renderCE)
            .fold(
              _ => assertNever("interpreter failed"),
              s => assert(s)(startsWithString("attribute_exists"))
            )
        },
        test("remove on Option[List] element renders REMOVE with index syntax") {
          val rendered = renderAction(Profile.tagAt(3).remove)
          assert(rendered)(startsWithString("remove") && containsString("[3]"))
        },
        test("set on Option[Map] key renders SET") {
          val rendered = renderAction(Profile.metaAt("views").set(42))
          assert(rendered)(startsWithString("set") && containsString("="))
        }
      )
    }
  )

  private def assertNever(msg: String) = assertTrue(msg == "impossible")
}
