import sbt._
import zio.json.ast.Json
import zio.sbt.ZioSbtCiPlugin._
import zio.sbt.githubactions.Step.SingleStep
import zio.sbt.githubactions._

// zio-sbt-ci's built-in `ciPublishSnapshots` toggle fires the release job on pushes to
// the *GitHub repo's default branch* — for this repo that's still `series/2.x`, not
// `series/3.x`, so the built-in toggle doesn't apply here. This mirrors zio-blocks'
// release job (custom condition + retry-wrapped release step), but targets
// `series/3.x` explicitly instead of relying on the default branch.
object CiWorkflow {

  private val pushToSeries3xCondition =
    Condition.Expression("github.event_name == 'push'") &&
      Condition.Expression("github.ref == 'refs/heads/series/3.x'")

  private val releaseOrSnapshotCondition = Some(
    Condition.Expression("github.event_name == 'release'") &&
      Condition.Expression("github.event.action == 'published'") || pushToSeries3xCondition
  )

  private val releaseRetryStep: SingleStep =
    SingleStep(
      name = "Release",
      uses = Some(ActionRef("nick-fields/retry@v4")),
      parameters = Map(
        "timeout_minutes" -> Json.Num(30),
        "max_attempts"    -> Json.Num(3),
        "command"         -> Json.Str("sbt ci-release")
      ),
      env = Map(
        "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
        "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
        "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
        "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
      )
    )

  // Takes the plugin's own default release job and swaps in the series/3.x-aware
  // condition plus a retry-wrapped release step, keeping every other generated
  // detail (checkout/setup/cache steps, job id/name/need) exactly as the plugin
  // would otherwise produce them.
  lazy val release: Def.Initialize[Job] = Def.setting {
    val default = releaseJobs.value.head
    default.copy(
      condition = releaseOrSnapshotCondition,
      steps = default.steps.dropRight(1) :+ releaseRetryStep
    )
  }
}
