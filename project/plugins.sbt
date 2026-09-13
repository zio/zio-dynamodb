addSbtPlugin("org.scalameta"      % "sbt-scalafmt"    % "2.5.6")
addSbtPlugin("com.github.sbt"     % "sbt-ci-release"  % "1.12.1")
addSbtPlugin("com.github.sbt"     % "sbt-header"      % "5.11.0")
addSbtPlugin("dev.zio"            % "zio-sbt-website" % "0.6.1")
addSbtPlugin("dev.zio"            % "zio-sbt-ci"      % "0.7.2")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"         % "0.4.7")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"   % "2.4.4")
// sbt-git used to arrive transitively via sbt-ci-release, which dropped that dependency in 1.12.0.
addSbtPlugin("com.github.sbt"     % "sbt-git"         % "2.2.0")
