package com.starbucks.analytics.parsercombinator

sealed trait UploaderCompilationError

case class UploaderLexerError(
  location: Location,
  msg:      String
) extends UploaderCompilationError

case class UploaderParserError(
  location: Location,
  msg:      String
) extends UploaderCompilationError

case class Location(
    line:   Int,
    column: Int
) {
  override def toString = s"$line:$column"
}