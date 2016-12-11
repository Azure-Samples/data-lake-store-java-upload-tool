package com.starbucks.analytics.parsercombinator

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{ NoPosition, Position, Reader }

/**
 * Responsible for syntactic analysis of the lexical analysis
 * and transform the sequence of tokens to the abstract
 * syntax tree (AST). For more information on AST, please visit
 * https://en.wikipedia.org/wiki/Abstract_syntax_tree
 */
object UploaderParser extends Parsers {
  override type Elem = UploaderToken

  //  def apply(tokens: Seq[UploaderToken]): Either[UploaderParserError, UploaderAST] = {
  //    val reader = new UploaderTokenReader(tokens)
  //        program(reader) match {
  //          case NoSuccess(msg, next)  => Left(UploaderParserError(Location(next.pos.line, next.pos.column), msg))
  //          case Success(result, next) => Right(result)
  //        }
  //  }

  /**
   * Used by the parser to read from the sequence of UploaderToken.
   * @param tokens Sequence of UploaderToken
   */
  class UploaderTokenReader(tokens: Seq[UploaderToken]) extends Reader[UploaderToken] {
    override def first: UploaderToken = tokens.head

    override def rest: Reader[UploaderToken] = new UploaderTokenReader(tokens.tail)

    override def pos: Position = NoPosition

    override def atEnd: Boolean = tokens.isEmpty
  }

  private def identifier: Parser[IDENTIFIER] = {
    accept("identifier", { case id @ IDENTIFIER(name) => id })
  }

  private def literal: Parser[LITERAL] = {
    accept("literal", { case lit @ LITERAL(name) => lit })
  }

}
