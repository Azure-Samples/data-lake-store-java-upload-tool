DefaultOptions.addPluginResolvers
resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

logLevel := Level.Warn