package com.starbucks.analytics.parsercombinator

import java.io.Reader

/**
 * Pipelines the lexer and the parser together
 */
object Compiler {
  def apply(reader: Reader): Either[UploaderCompilationError, UploaderAST] = {
    for {
      tokens <- UploaderLexer(reader).right
      ast <- UploaderParser(tokens).right
    } yield ast
  }
}

