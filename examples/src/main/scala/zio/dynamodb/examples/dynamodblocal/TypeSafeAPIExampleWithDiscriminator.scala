package zio.dynamodb.examples.dynamodblocal

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.blocking.Blocking
import zio.clock.Clock
import zio.dynamodb.Annotations.discriminator
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.examples.LocalDdbServer
import zio.dynamodb.examples.dynamodblocal.TypeSafeAPIExampleWithDiscriminator.TrafficLight.{ Amber, Box, Green }
import zio.schema.DeriveSchema
import zio.{ App, ExitCode, Has, URIO, ZIO, ZLayer }

import java.net.URI

object TypeSafeAPIExampleWithDiscriminator extends App {

  @discriminator("light_type")
  sealed trait TrafficLight

  object TrafficLight {
    final case class Green(rgb: Int) extends TrafficLight

    object Green {
      implicit val schema = DeriveSchema.gen[Green]
      val rgb             = ProjectionExpression.accessors[Green]
    }

    final case class Red(rgb: Int) extends TrafficLight

    object Red {
      implicit val schema = DeriveSchema.gen[Red]
      val rgb             = ProjectionExpression.accessors[Red]
    }

    final case class Amber(rgb: Int) extends TrafficLight

    object Amber {
      implicit val schema = DeriveSchema.gen[Amber]
      val rgb             = ProjectionExpression.accessors[Amber]
    }

    final case class Box(id: Int, code: Int, trafficLightColour: TrafficLight)

    object Box {
      implicit val schema                = DeriveSchema.gen[Box]
      val (id, code, trafficLightColour) = ProjectionExpression.accessors[Box]
    }

    implicit val schema     = DeriveSchema.gen[TrafficLight]
    val (amber, green, red) = ProjectionExpression.accessors[TrafficLight]
  }

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (http4s.default ++ awsConfig) >>> config.configured() >>> dynamodb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val layer = ((dynamoDbLayer ++ ZLayer.identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live) ++ (ZLayer
    .identity[Has[Blocking.Service]] >>> LocalDdbServer.inMemoryLayer)

  private val program = for {
    _         <- createTable("box", KeySchema("id", "code"), BillingMode.PayPerRequest)(
                   AttributeDefinition.attrDefnNumber("id"),
                   AttributeDefinition.attrDefnNumber("code")
                 ).execute
    boxOfGreen = Box(1, 1, Green(1))
    boxOfAmber = Box(1, 2, Amber(1))
    _         <- put[Box]("box", boxOfGreen).execute
    _         <- put[Box]("box", boxOfAmber).execute
    query      = queryAll[Box]("box")
                   .whereKey(Box.id === 1)
                   .filter(Box.trafficLightColour >>> TrafficLight.green >>> Green.rgb === 1)
    stream    <- query.execute
    list      <- stream.runCollect
    _         <- ZIO.debug(s"boxes=$list")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
