package com.starbucks.analytics.parsercombinator

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers

/**
 * Responsible for lexical & syntactic analysis of the internal DSL
 * used for uploading
 */
object InterpolationParser extends RegexParsers {
  var declarationMap: mutable.Map[String, Token] = mutable.Map[String, Token]()

  /**
   * Parsers the input stream and presents the results
   * of parsing the content in the interpolation string
   *
   * @param reader Input reader
   * @return Result of lexing
   */
  def parse(
    reader:       String,
    declarations: mutable.Map[String, Token]
  ): Either[UploaderParserError, String] = {
    declarationMap = declarations
    val parsed = parseAll(block, reader)
    parsed match {
      case NoSuccess(msg, next) => Left(
        UploaderParserError(
          Location(next.pos.line, next.pos.column),
          msg
        )
      )
      case Success(result, _) => Right(result)
    }
  }

  // Combinators for lexing the string interpolation
  private def literalToken: Parser[LITERAL] = {
    """"[^"]*"""".r ^^ { str =>
      val content = str.substring(1, str.length - 1)
      LITERAL(content)
    }
  }

  private def variableToken: Parser[VARIABLE] = {
    "^\\$[a-zA-Z0-9_]*".r ^^ { str =>
      val content = str.substring(1)
      VARIABLE(content)
    }
  }

  // Combinator that brings it all together
  private def block: Parser[String] = {
    rep1(literalToken | variableToken) ^^ { listOfTokens =>
      val builder = new StringBuilder
      listOfTokens.foreach {
        case LITERAL(lit) =>
          builder ++= lit
        case VARIABLE(v) =>
          if (declarationMap.contains(v)) {
            declarationMap(v) match {
              case INTERPOLATION(i) =>
                val result = InterpolationParser.parse(i, declarationMap)
                if (result.isRight) {
                  builder ++= result.right.get
                }
              case LITERAL(lit) =>
                builder ++= lit
              case unknown =>
                throw new Exception(s"The variable $v is defined but don't know how to parse $unknown.")
            }
          } else {
            throw new Exception(s"The variable $v is not defined.")
          }
        case token @ unknown =>
          throw new Exception(s"Evaluating interpolation of $token encountered an object $unknown.")
      }
      builder.toString
    }
  }
}