ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

Compile / sourceGenerators += (Compile / avroScalaGenerate).taskValue
Test / sourceGenerators += (Test / avroScalaGenerate).taskValue

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    name := "kfkStrmz",
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.module" %% "jackson-module-scala"     % "2.14.2",
      "org.apache.kafka"              % "kafka-streams"            % "3.4.0",
      "org.apache.kafka"             %% "kafka-streams-scala"      % "3.4.0",
      "org.apache.kafka"              % "kafka-streams-test-utils" % "3.4.0",
      "org.log4s"                    %% "log4s"                    % "1.10.0",
      "ch.qos.logback"                % "logback-classic"          % "1.4.5",
      "org.apache.avro"               % "avro"                     % "1.11.0",
      "org.scalatest"                %% "scalatest"                % "3.2.15" % "it,test"
    )
  )
