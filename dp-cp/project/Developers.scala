object Developers {
  lazy val members = Map(
    "gandhinath" -> "Gandhinath Swaminathan"
  )

  def toXml =
    <developers>
      {members map { m =>
      <developer>
        <id>
          {m._1}
        </id>
        <name>
          {m._2}
        </name>
        <url>http://github.com/
          {m._1}
        </url>
      </developer>
    }}
    </developers>
}