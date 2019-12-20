name := "johu-job-scraper"

version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.5.25"
lazy val reactiveMongoVersion = "0.19.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.reactivemongo" %% "reactivemongo" % reactiveMongoVersion,

  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

scalacOptions := Seq("-unchecked", "-deprecation")
