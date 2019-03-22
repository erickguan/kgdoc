import sbt._
import Keys.{libraryDependencies, scalaVersion, _}
import Dependencies._

val scioVersion = "0.7.3"
val beamVersion = "2.10.0"
val circeVersion = "0.10.0"

scalaVersion := "2.12.8"
val scalaMacroVersion = "2.12.8"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "me.erickguan.kgdoc",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := scalaMacroVersion,
  scalacOptions ++= Seq("-target:jvm-1.8",
                        "-deprecation",
                        "-feature",
                        "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaMacroVersion,
  addCompilerPlugin(paradiseDependency)
)


lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    name := "kgdoc",
    description := "kgdoc",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-extra" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      // optional dataflow runner
      "org.apache.beam" % "beam-runners-spark" % beamVersion,
      //      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      loggerLib % Compile,
      confLib % Compile,
      "io.circe" %% "circe-core" % circeVersion % Compile,
      "io.circe" %% "circe-generic" % circeVersion % Compile,
      "io.circe" %% "circe-generic-extras" % circeVersion % Compile,
      "io.circe" %% "circe-parser" % circeVersion % Compile,
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  )
  .enablePlugins(PackPlugin)

lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for kgdoc",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
    publish / skip := true
  )
  .dependsOn(root)

