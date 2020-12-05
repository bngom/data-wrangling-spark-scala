ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "com.bn"


val scalaTest = "org.scalatest" %% "scalatest" % "3.2.2"

lazy val app = (project in file("."))
  .settings(
    name := "App",
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1",
    libraryDependencies += scalaTest % "test",
  )

assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
assemblyJarName in assembly := "LogReporting-assembly-1.0.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
