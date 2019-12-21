name := "johu-job-scraper"

version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.5.25"
lazy val reactiveMongoVersion = "0.19.5"
lazy val jSoupVersion  ="1.12.1"
lazy val scalaTestVersion = "3.0.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.reactivemongo" %% "reactivemongo" % reactiveMongoVersion,

  "org.jsoup" % "jsoup" % jSoupVersion,

  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

scalacOptions := Seq("-unchecked", "-deprecation")
