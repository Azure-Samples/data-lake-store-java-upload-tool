DefaultOptions.addPluginResolvers
resolvers ++= Seq(
  Resolver.typesafeRepo("releases"))

//addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

logLevel := Level.Debug