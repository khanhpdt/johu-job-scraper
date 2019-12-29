name := "johu-job-scraper"
version := "1.0.0"
scalaVersion := "2.13.1"

enablePlugins(JavaAppPackaging)

lazy val akkaVersion = "2.5.25"
lazy val akkaHttpVersion = "10.1.9"
lazy val reactiveMongoVersion = "0.19.5"
lazy val jSoupVersion = "1.12.1"
lazy val scalaTestVersion = "3.0.8"
lazy val rabbitMqVersion = "5.8.0"
lazy val circeVersion = "0.12.3"
lazy val quartzVersion = "2.3.2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.reactivemongo" %% "reactivemongo" % reactiveMongoVersion,

  "org.jsoup" % "jsoup" % jSoupVersion,

  "com.rabbitmq" % "amqp-client" % rabbitMqVersion,

  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,

  "org.quartz-scheduler" % "quartz" % quartzVersion,

  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion, // required for akka-http

  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

scalacOptions := Seq("-unchecked", "-deprecation")

dockerBaseImage := "openjdk:8"

javaOptions in Universal ++= Seq(
  // JVM memory tuning
  "-J-Xmx1024m",
  "-J-Xms512m",

  // Use separate configuration file for production environment
  s"-Dconfig.resource=production.conf",

  // Use separate logger configuration file for production environment
  s"-Dlogback.configurationFile=logback-prod.xml"
)

dockerExposedPorts ++= Seq(9090)
