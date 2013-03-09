import sbt._
import Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.{ MultiJvm }


object Pakkxos extends Build {

  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.github.bigtoast",
    version      := "0.1.0",
    scalaVersion := "2.10.0",
    crossPaths   := false
  )

	val akka    = "com.typesafe.akka" %% "akka-actor" % "2.1.0"
	val remote  = "com.typesafe.akka" %% "akka-remote" % "2.1.0"
	val cluster = "com.typesafe.akka" %% "akka-cluster-experimental" % "2.1.0"

  // test deps
  val rTests    = "com.typesafe.akka" %% "akka-remote-tests-experimental" % "2.1.0" % "test"
  val testKit   = "com.typesafe.akka" %% "akka-testkit" % "2.1.0" % "test"

  val scalaTest = "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"
	
	lazy val pakkxos = Project( id = "pakkxos", base = file(".") )
		.settings( buildSettings :_* )
		.aggregate("dist-actors")

	lazy val distActors = Project(
    id       = "dist-actors",
    base     = file("dist-actors"),
    settings = buildSettings ++ multiJvmSettings ++ Seq (
			libraryDependencies ++= 
				akka :: remote :: cluster :: scalaTest :: rTests :: testKit :: Nil

    )).configs(MultiJvm)


  lazy val multiJvmSettings = SbtMultiJvm.multiJvmSettings ++ Seq(
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    // disable parallel tests
    parallelExecution in Test := false,
    // make sure that MultiJvm tests are executed by the default test target
    executeTests in Test <<=
      ((executeTests in Test), (executeTests in MultiJvm)) map {
        case ((_, testResults), (_, multiJvmResults))  =>
          val results = testResults ++ multiJvmResults
          (Tests.overall(results.values), results)
      }
  )
}