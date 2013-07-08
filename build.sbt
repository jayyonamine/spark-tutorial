import AssemblyKeys._ 

assemblySettings

name := "SparkTutorial"

version := "0.1"

scalaVersion := "2.9.3"


resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/",
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
        "releases" at "http://oss.sonatype.org/content/repositories/releases"
)


resolvers += "typesafe" at "http://repo.typesafe.com/typesafe/releases"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.2" % "provided"

runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

