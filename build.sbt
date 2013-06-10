import AssemblyKeys._ 

assemblySettings

name := "Spark Tutorial"

version := "0.1"

scalaVersion := "2.9.2"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.0"

resolvers += "spray.cc" at "http://repo.spray.cc"

resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases"

mergeStrategy in assembly := { case _ => MergeStrategy.first }
