import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.github.io/zio-dynamodb/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      ),
      Developer(
        "googley42",
        "Avinder Bahra",
        "avinder.bahra@gmail.com",
        url("https://github.com/googley42")
      )
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/googley42/zio-dynamodb"), "scm:git:git@github.com:googley42/zio-dynamodb.git")
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion       = "1.0.8"
val zioConfigVersion = "1.0.0-RC30-1"

lazy val root =
  project
    .in(file("."))
    .settings(skip in publish := true)
    .aggregate(zioDynamodb, examples)

lazy val zioDynamodb = module("zio-dynamodb", "dynamodb")
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoSettings("zio.dynamodb"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"       %% "zio"                 % zioVersion,
      "dev.zio"       %% "zio-streams"         % zioVersion,
      "dev.zio"       %% "zio-test"            % zioVersion % "test",
      "dev.zio"       %% "zio-test-sbt"        % zioVersion % "test",
      "dev.zio"       %% "zio-config-typesafe" % zioConfigVersion,
      "org.scala-lang" % "scala-reflect"       % scalaVersion.value
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedAttrMapApplies.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      def lowerAlpha(i: Int): Char = (('a'.toInt - 1) + i).toChar
      val applyMethods             = (1 to 22).map {
        i =>
          val types     = (1 to i).map(upperAlpha).mkString(", ")
          val tparams   = (1 to i).map(p => s"t$p: (String, ${upperAlpha(p)})").mkString(", ")
          val implicits = (1 to i).map(p => s"${lowerAlpha(p)}: ToAttributeValue[${upperAlpha(p)}]").mkString(", ")
          val tentries  = (1 to i).map(p => s"t$p._1 -> ${lowerAlpha(p)}.toAttributeValue(t$p._2)").mkString(", ")

          s"""def apply[$types]($tparams)(implicit $implicits): AttrMap =
             |    AttrMap(
             |      Map($tentries)
             |    )""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |private[dynamodb] trait GeneratedAttrMapApplies {
           |  
           |  ${applyMethods.mkString("\n\n  ")}
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue
  )
  .settings(
    stdSettings("zio-dynamodb")
  )

lazy val examples = module("zio-dynamodb-examples", "examples")
  .settings(
    skip in publish := true,
    fork := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % "test",
      "dev.zio" %% "zio-test-sbt" % zioVersion % "test"
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(zioDynamodb)

def module(moduleName: String, fileName: String): Project =
  Project(moduleName, file(fileName))
    .settings(stdSettings(moduleName))
    .settings(
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio" % zioVersion
      )
    )

lazy val docs = project
  .in(file("zio-dynamodb-docs"))
  .settings(
    skip.in(publish) := true,
    moduleName := "zio-dynamodb-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    ),
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(root),
    target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
    cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(unidoc in Compile).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(unidoc in Compile).value
  )
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
