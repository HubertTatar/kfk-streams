ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "kfkStrmz",
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.2",
      "org.apache.kafka" % "kafka-streams"        % "3.1.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0",
      "org.log4s"        %% "log4s"               % "1.10.0",
      "ch.qos.logback"   % "logback-classic"      % "1.2.10"
    )
  )




