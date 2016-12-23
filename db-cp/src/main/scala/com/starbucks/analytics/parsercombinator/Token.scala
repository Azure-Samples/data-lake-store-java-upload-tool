package com.starbucks.analytics.parsercombinator

import scala.util.parsing.input.Positional

/**
 * Tokens to represent string literals,reserved keywords, identifiers
 */
sealed trait Token extends Positional

case class EMPTY() extends Token
case class LITERAL(str: String) extends Token
case class IDENTIFIER(str: String) extends Token
case class VARIABLE(str: String) extends Token
case class INTERPOLATION(str: String) extends Token
case class SQL(str: String) extends Token
case class OPERATOR(op: String) extends Token
case class FUNCTION(fun: String) extends Token
case class COMMENT(str: String) extends Token
case class ASSIGNMENT() extends Token
// Parameter Tokens
case class WITH() extends Token
case class USERNAME() extends Token
case class PASSWORD() extends Token
case class DRIVER() extends Token
case class SOURCE() extends Token
case class CLIENT_ID() extends Token
case class AUTH_TOKEN_ENDPOINT() extends Token
case class CLIENT_KEY() extends Token
case class TARGET() extends Token
// Source To Target Tokens
case class USING() extends Token
case class SELECT() extends Token
case class OWNER() extends Token
case class TABLES() extends Token
case class PARTITIONS() extends Token
case class SUB_PARTITIONS() extends Token
case class PREDICATE() extends Token
case class UPLOAD() extends Token
// Options
case class OPTIONS() extends Token
case class DESIRED_BUFFER_SIZE() extends Token
case class DESIRED_PARALLELISM() extends Token
case class FETCH_SIZE() extends Token
case class SEPARATOR() extends Token
case class ROWSEPARATOR() extends Token
case class QUOTE() extends Token
