package com.starbucks.analytics.parsercombinator

import scala.util.parsing.input.Positional

/**
 * Tokens to represent string literals,reserved keywords, identifiers
 */
sealed trait UploaderToken extends Positional

case class LITERAL(str: String) extends UploaderToken
case class IDENTIFIER(str: String) extends UploaderToken
case class VARIABLE(str: String) extends UploaderToken
case class INTERPOLATION(str: String) extends UploaderToken
case class SQL(str: String) extends UploaderToken
case class ASSIGNMENT() extends UploaderToken
// Parameter Tokens
case class WITH() extends UploaderToken
case class USERNAME() extends UploaderToken
case class PASSWORD() extends UploaderToken
case class DRIVER() extends UploaderToken
case class SOURCE() extends UploaderToken
case class CLIENT_ID() extends UploaderToken
case class AUTH_TOKEN_ENDPOINT() extends UploaderToken
case class CLIENT_KEY() extends UploaderToken
case class TARGET() extends UploaderToken
// Source To Target Tokens
case class USING() extends UploaderToken
case class SELECT() extends UploaderToken
case class OWNER() extends UploaderToken
case class TABLES() extends UploaderToken
case class PARTITIONS() extends UploaderToken
case class SUB_PARTITIONS() extends UploaderToken
case class PREDICATE() extends UploaderToken
case class EQUALS() extends UploaderToken
case class UPLOAD() extends UploaderToken
// Options
case class OPTIONS() extends UploaderToken
case class DESIRED_BUFFER_SIZE() extends UploaderToken
case class DESIRED_PARALLELISM() extends UploaderToken
case class FETCH_SIZE() extends UploaderToken
case class SEPARATOR() extends UploaderToken
case class QUOTE() extends UploaderToken
