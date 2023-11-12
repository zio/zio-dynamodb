package zio.dynamodb

import zio.Scope
import zio.dynamodb.ProjectionExpression.$
import zio.test._

object AliasMapSpec extends ZIOSpecDefault {

  private val root: ProjectionExpression[Any, Any]        = ProjectionExpression.Root
  private def rootPathSegment(path: String): AliasMap.Key = pathSegment(root, path)

  private def pathSegment[From, To](pe: ProjectionExpression[From, To], path: String): AliasMap.Key =
    AliasMap.PathSegment(pe, path)

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("AliasMap getOrInsert a path suite")(
      test("renders a simple projection expression") {
        val map = Map(
          rootPathSegment("abc") -> "#n0"
        )

        val (aliasMap, s) = AliasMap.empty.getOrInsert($("abc"))

        assertTrue(s == "#n0" && aliasMap.map == map)
      },
      test("renders a Map projection expression") {
        val pe  = $("map.abc")
        val map = Map(
          rootPathSegment("map")       -> "#n1",
          pathSegment($("map"), "abc") -> "#n0"
        )

        val (aliasMap, s) = AliasMap.empty.getOrInsert(pe)

        assertTrue(s == "#n1.#n0" && aliasMap.map == map)
      },
      test("renders an Array projection expression") {
        val pe  = $("names[10]")
        val map = Map(rootPathSegment("names") -> "#n0")

        val (aliasMap, s) = AliasMap.empty.getOrInsert(pe)

        assertTrue(s == "#n0[10]" && aliasMap.map == map)
      },
      test("renders an Array composite projection expression") {
        val pe  = $("names[10].address")
        val map = Map(
          rootPathSegment("names")               -> "#n1",
          pathSegment($("names[10]"), "address") -> "#n0"
        )

        val (aliasMap, s) = AliasMap.empty.getOrInsert(pe)

        assertTrue(s == "#n1[10].#n0" && aliasMap.map == map)
      },
      test("renders a composite Map and Array projection expression") {
        val pe  = $("map.names[10].address")
        val map = Map(
          rootPathSegment("map")                     -> "#n2",
          pathSegment($("map"), "names")             -> "#n1",
          pathSegment($("map.names[10]"), "address") -> "#n0"
        )

        val (aliasMap, s) = AliasMap.empty.getOrInsert(pe)

        assertTrue(s == "#n2.#n1[10].#n0" && aliasMap.map == map)
      }
    )
}
