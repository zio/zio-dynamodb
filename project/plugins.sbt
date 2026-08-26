resolvers += Resolver.sonatypeCentralSnapshots

addSbtPlugin("org.scalameta"      % "sbt-scalafmt"    % "2.6.2")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"         % "0.4.8")
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo"   % "0.13.1")
addSbtPlugin("org.scoverage"      % "sbt-scoverage"   % "2.4.3")
addSbtPlugin("com.github.sbt"     % "sbt-ci-release"  % "1.12.0")
addSbtPlugin("dev.zio"            % "zio-sbt-website" % "0.7.2+2-33329252-SNAPSHOT")
addSbtPlugin("dev.zio"            % "zio-sbt-ci"      % "0.7.2+2-33329252-SNAPSHOT")
