resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")