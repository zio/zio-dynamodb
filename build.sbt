import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.dev/zio-dynamodb/")),
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
      ScmInfo(url("https://github.com/zio/zio-dynamodb"), "scm:git:git@github.com:zio/zio-dynamodb.git")
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion             = "2.0.21"
val zioAwsVersion          = "7.21.15.12"
val zioSchemaVersion       = "1.1.0"
val zioPreludeVersion      = "1.0.0-RC23"
val zioInteropCats3Version = "23.1.0.1"
val catsEffect3Version     = "3.5.3"
val fs2Version             = "3.9.4"

lazy val root =
  project
    .in(file("."))
    .settings(skip in publish := true)
    .aggregate(zioDynamodb, zioDynamodbCe, zioDynamodbJson, examples /*, docs */ )

lazy val zioDynamodb = module("zio-dynamodb", "dynamodb")
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoSettings("zio.dynamodb"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    resolvers += Resolver.sonatypeRepo("releases"),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"                   % zioVersion,
      "dev.zio" %% "zio-prelude"           % zioPreludeVersion,
      "dev.zio" %% "zio-streams"           % zioVersion,
      "dev.zio" %% "zio-test"              % zioVersion % "it,test",
      "dev.zio" %% "zio-test-sbt"          % zioVersion % "it,test",
      "dev.zio" %% "zio-schema"            % zioSchemaVersion,
      "dev.zio" %% "zio-schema-derivation" % zioSchemaVersion,
      "dev.zio" %% "zio-aws-netty"         % zioAwsVersion,
      "dev.zio" %% "zio-aws-dynamodb"      % zioAwsVersion
    ),
    libraryDependencies ++= {
      if (scalaVersion.value == Scala3) Seq()
      else
        Seq(
          "org.scala-lang" % "scala-reflect" % scalaVersion.value
        )
    },
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedAttrMapApplies.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      def lowerAlpha(i: Int): Char = (('a'.toInt - 1) + i).toChar
      val applyMethods             = (1 to 22).map { i =>
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
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "Zippable.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      val implicitZippables        = (3 to 22).map { i =>
        val types      = (1 until i).map(upperAlpha).mkString(", ")
        val leftTuples = (1 until i).map(i => s"left._$i").mkString(", ")

        s"""implicit def Zippable$i[$types, Z]: Zippable.Out[($types), Z, ($types, Z)] =
           |    new Zippable[($types), Z] {
           |      type Out = ($types, Z)
           |
           |      def zip(left: ($types), right: Z): Out = ($leftTuples, right)
           |    }
           |""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |sealed trait Zippable[-A, -B] {
           |  type Out
           |
           |  def zip(left: A, right: B): Out
           |}
           |object Zippable extends ZippableLowPriority1 {
           |  type Out[-A, -B, C] = Zippable[A, B] { type Out = C }
           |
           |  implicit def ZippableUnit[A]: Zippable.Out[A, Unit, A] =
           |    new Zippable[A, Unit] {
           |      type Out = A
           |
           |      def zip(left: A, right: Unit): Out = left
           |    }
           |
           |}
           |trait ZippableLowPriority1 extends ZippableLowPriority2 {
           |  ${implicitZippables.mkString("\n  ")}
           |
           |}
           |trait ZippableLowPriority2 extends ZippableLowPriority3 {
           |
           |  implicit def Zippable2Right[B]: Zippable.Out[Unit, B, B] =
           |    new Zippable[Unit, B] {
           |      type Out = B
           |
           |      def zip(left: Unit, right: B): Out = right
           |    }
           |}
           |trait ZippableLowPriority3 extends ZippableLowPriority4 {
           |  implicit def Zippable2Left[A]: Zippable.Out[A, Unit, A] =
           |    new Zippable[A, Unit] {
           |      type Out = A
           |
           |      def zip(left: A, right: Unit): Out = left
           |    }
           |
           |}
           |
           |trait ZippableLowPriority4 {
           |  implicit def Zippable2[A, B]: Zippable.Out[A, B, (A, B)] =
           |    new Zippable[A, B] {
           |      type Out = (A, B)
           |
           |      def zip(left: A, right: B): Out = (left, right)
           |    }
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedFromAttributeValueAs.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      def lowerAlpha(i: Int): Char = (('a'.toInt - 1) + i).toChar
      val applyMethods             = (2 to 22).map { i =>
        val returnType = upperAlpha(i + 1)
        val tparams    = (1 to i).map(p => s"${upperAlpha(p)}: FromAttributeValue").mkString(", ")
        val params     = (1 to i).map(p => s"field$p: String").mkString(",\n    ")
        val ftypes     = (1 to i).map(p => s"${upperAlpha(p)}").mkString(", ")
        val fparams    = (1 to i).map(p => s"${lowerAlpha(p)}").mkString(", ")
        val gets       = (1 to i).map(p => s"${lowerAlpha(p)} <- get[${upperAlpha(p)}](field$p)").mkString("\n      ")
        s"""def as[$tparams, $returnType](
           |    $params
           |  )(fn: ($ftypes) => $returnType): Either[ItemError, $returnType] =
           |    for {
           |      $gets
           |    } yield fn($fparams)""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |import zio.dynamodb.DynamoDBError.ItemError
           |
           |private[dynamodb] trait GeneratedFromAttributeValueAs { this: AttrMap =>
           |
           |  ${applyMethods.mkString("\n\n  ")}
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedFromAttributeValueAs.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      def lowerAlpha(i: Int): Char = (('a'.toInt - 1) + i).toChar
      val applyMethods             = (2 to 22).map { i =>
        val returnType = upperAlpha(i + 1)
        val tparams    = (1 to i).map(p => s"${upperAlpha(p)}: FromAttributeValue").mkString(", ")
        val params     = (1 to i).map(p => s"field$p: String").mkString(",\n    ")
        val ftypes     = (1 to i).map(p => s"${upperAlpha(p)}").mkString(", ")
        val fparams    = (1 to i).map(p => s"${lowerAlpha(p)}").mkString(", ")
        val gets       = (1 to i).map(p => s"${lowerAlpha(p)} <- get[${upperAlpha(p)}](field$p)").mkString("\n      ")
        s"""def as[$tparams, $returnType](
           |    $params
           |  )(fn: ($ftypes) => $returnType): Either[ItemError, $returnType] =
           |    for {
           |      $gets
           |    } yield fn($fparams)""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |import zio.dynamodb.DynamoDBError.ItemError
           |
           |private[dynamodb] trait GeneratedFromAttributeValueAs { this: AttrMap =>
           |
           |  ${applyMethods.mkString("\n\n  ")}
           |}""".stripMargin
      )
      Seq(file)
    }.taskValue,
    Compile / sourceGenerators += Def.task {
      val dir                      = (Compile / sourceManaged).value
      val file                     = dir / "zio" / "dynamodb" / "GeneratedCaseClassDecoders.scala"
      def upperAlpha(i: Int): Char = (('A'.toInt - 1) + i).toChar
      val applyMethods             = (1 to 22).map { i =>
        val constructorParams = (1 to i)
          .map(p => s"xs(${p - 1}).asInstanceOf[${upperAlpha(p)}]")
          .mkString(", ")
        val fieldParams       = (1 to i).map(p => s"schema.field${if (i == 1) "" else p.toString}").mkString(",")
        val fieldTypes        = (1 to i).map(p => s"${upperAlpha(p)}").mkString(", ")
        val schemaConstruct   = if (i == 1) "schema.defaultConstruct" else "schema.construct"
        s"""def caseClass${i}Decoder[$fieldTypes, Z](schema: Schema.CaseClass${i}[$fieldTypes, Z]): Decoder[Z] =  { (av: AttributeValue) =>
           |    Codec.Decoder.decodeFields(av, $fieldParams).map { xs =>
           |      $schemaConstruct($constructorParams)
           |    }
           |  }""".stripMargin
      }
      IO.write(
        file,
        s"""package zio.dynamodb
           |
           |import zio.schema.Schema
           |
           |private[dynamodb] trait GeneratedCaseClassDecoders {
           |
           |  ${applyMethods.mkString("\n\n  ")}
           |
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
    resolvers += Resolver.sonatypeRepo("releases"),
    skip in publish := true,
    fork := true,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect"  % catsEffect3Version,
      "co.fs2"        %% "fs2-core"     % fs2Version,
      "dev.zio"       %% "zio-test"     % zioVersion % "test",
      "dev.zio"       %% "zio-test-sbt" % zioVersion % "test"
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(zioDynamodb, zioDynamodbCe, zioDynamodbJson)

lazy val zioDynamodbCe =
  module("zio-dynamodb-ce", "interop/dynamodb-ce")
    .enablePlugins(BuildInfoPlugin)
    .settings(buildInfoSettings("zio.dynamodb"))
    .configs(IntegrationTest)
    .settings(
      resolvers += Resolver.sonatypeRepo("releases"),
      fork := true,
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-effect"      % catsEffect3Version,
        "co.fs2"        %% "fs2-core"         % fs2Version,
        "dev.zio"       %% "zio-test"         % zioVersion % "test",
        "dev.zio"       %% "zio-test-sbt"     % zioVersion % "test",
        "dev.zio"       %% "zio-interop-cats" % zioInteropCats3Version
      ),
      testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
    )
    .dependsOn(zioDynamodb)

lazy val zioDynamodbJson =
  module("zio-dynamodb-json", "dynamodb-json")
    .enablePlugins(BuildInfoPlugin)
    .settings(buildInfoSettings("zio.dynamodb"))
    .configs(IntegrationTest)
    .settings(
      resolvers += Resolver.sonatypeRepo("releases"),
      fork := true,
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio-test"     % zioVersion % "test",
        "dev.zio" %% "zio-test-sbt" % zioVersion % "test",
        "dev.zio" %% "zio-json"     % "0.6.2"
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
    moduleName := "zio-dynamodb-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq("dev.zio" %% "zio" % zioVersion),
    projectName := "ZIO DynamoDB",
    mainModuleName := (zioDynamodb / moduleName).value,
    projectStage := ProjectStage.Development,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(zioDynamodb),
    docsPublishBranch := "series/2.x"
  )
  .dependsOn(zioDynamodb)
  .enablePlugins(WebsitePlugin)
