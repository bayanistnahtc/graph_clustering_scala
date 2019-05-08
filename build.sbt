name := "GraphClusteringScala"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.2"
val breezeVersion = "0.13.2"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion



//libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
