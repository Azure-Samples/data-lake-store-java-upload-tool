package com.starbucks.analytics.parsercombinator

import java.io.Reader

import scala.util.parsing.combinator.RegexParsers

/**
 * Character parser using regular expressions to parse the input
 */
object UploaderLexer extends RegexParsers {
  // Ignore any whitespace character
  override def skipWhitespace = true

  /**
   * Parsers the input stream and presents the results
   * of lexing the content in the stream
   *
   * @param reader Input reader
   * @return Result of lexing
   */
  def parse(reader: Reader) = {
    parseAll(block, reader)
  }

  /**
   * Compose a parser capable of recognizing the internal DSL.
   */
  def block = opt(rep1(declaration)) ~ statement

  def declaration = variableToken ~ assignmentToken ~ (interpolationToken | sqlToken)

  def statement = setup ~ using ~ select ~ upload ~ option

  def setup = withToken ~ username ~ password ~ driver ~ source ~
    clientId ~ authTokenEndPoint ~ clientKey ~ target

  def using = usingToken ~ identifierToken

  def select = selectToken ~ owner ~ table ~ opt(partition) ~
    opt(subPartition) ~ opt(predicate)

  def upload = uploadToken ~ (interpolationToken | identifierToken)

  def option = optionsToken ~ desiredBufferSize ~ desiredParallelism ~
    fetchSize ~ separator

  // declaration tokens
  def interpolationToken: Parser[INTERPOLATION] = {
    "^\\{.*\\}".r ^^ { str =>
      val content = str.substring(1, str.length - 1)
      INTERPOLATION(content)
    }
  }

  def sqlToken: Parser[SQL] = {
    "^\\(SELECT .*\\)".r ^^ { str =>
      val content = str.substring(1, str.length - 1)
      SQL(content)
    }
  }

  // setup tokens
  def username = usernameToken ~ literalToken

  def password = passwordToken ~ literalToken

  def driver = driverToken ~ literalToken

  def source = sourceToken ~ literalToken

  def clientId = clientIdToken ~ literalToken

  def authTokenEndPoint = authToKenEndPointToken ~ literalToken

  def clientKey = clientKeyToken ~ literalToken

  def target = targetToken ~ literalToken

  // select tokens
  def owner = ownerToken ~ identifierToken

  def table = tableToken ~ identifierToken

  def partition = partitionsToken ~ identifierToken

  def subPartition = subPartitionsToken ~ identifierToken

  def predicate = predicateToken ~ identifierToken ~
    equalsToken ~ opt(quoteToken) ~ variableToken ~ opt(quoteToken)

  // options token
  def desiredBufferSize = desiredBufferSizeToken ~ literalToken

  def desiredParallelism = desiredParallelismToken ~ literalToken

  def fetchSize = fetchSizeToken ~ literalToken

  def separator = separatorToken ~ literalToken

  def identifierToken: Parser[IDENTIFIER] = {
    "[a-zA-Z0-9_,\\/\\.]*".r ^^ { str => IDENTIFIER(str) }
  }

  def literalToken: Parser[LITERAL] = {
    """"[^"]*"""".r ^^ { str =>
      val content = str.substring(1, str.length - 1)
      LITERAL(content)
    }
  }

  def variableToken: Parser[VARIABLE] = {
    "^\\$[a-zA-Z0-9_]*".r ^^ { str =>
      val content = str.substring(1)
      VARIABLE(content)
    }
  }

  def assignmentToken = positioned {
    ":=" ^^ (_ => ASSIGNMENT())
  }

  def withToken = positioned {
    "WITH" ^^ (_ => WITH())
  }

  def usernameToken = positioned {
    "USERNAME" ^^ (_ => USERNAME())
  }

  def passwordToken = positioned {
    "PASSWORD" ^^ (_ => PASSWORD())
  }

  def driverToken = positioned {
    "DRIVER" ^^ (_ => DRIVER())
  }

  def sourceToken = positioned {
    "SOURCE" ^^ (_ => SOURCE())
  }

  def clientIdToken = positioned {
    "CLIENTID" ^^ (_ => CLIENT_ID())
  }

  def authToKenEndPointToken = positioned {
    "AUTHTOKENENDPOINT" ^^ (_ => AUTH_TOKEN_ENDPOINT())
  }

  def clientKeyToken = positioned {
    "CLIENTKEY" ^^ (_ => CLIENT_KEY())
  }

  def targetToken = positioned {
    "TARGET" ^^ (_ => TARGET())
  }

  def usingToken = positioned {
    "USING" ^^ (_ => USING())
  }

  def selectToken = positioned {
    "SELECT FROM" ^^ (_ => SELECT())
  }

  def ownerToken = positioned {
    "OWNER" ^^ (_ => OWNER())
  }

  def tableToken = positioned {
    "TABLES" ^^ (_ => TABLES())
  }

  def partitionsToken = positioned {
    "PARTITIONS" ^^ (_ => PARTITIONS())
  }

  def subPartitionsToken = positioned {
    "SUBPARTITIONS" ^^ (_ => SUB_PARTITIONS())
  }

  def predicateToken = positioned {
    "PREDICATE" ^^ (_ => PREDICATE())
  }

  def equalsToken = positioned {
    "=" ^^ (_ => EQUALS())
  }

  def uploadToken = positioned {
    "UPLOAD TO" ^^ (_ => UPLOAD())
  }

  def optionsToken = positioned {
    "OPTIONS" ^^ (_ => OPTIONS())
  }

  def desiredBufferSizeToken = positioned {
    "DESIREDBUFFERSIZE" ^^ (_ => DESIRED_BUFFER_SIZE())
  }

  def desiredParallelismToken = positioned {
    "DESIREDPARALLELISM" ^^ (_ => DESIRED_PARALLELISM())
  }

  def fetchSizeToken = positioned {
    "FETCHSIZE" ^^ (_ => FETCH_SIZE())
  }

  def separatorToken = positioned {
    "SEPARATOR" ^^ (_ => SEPARATOR())
  }

  def quoteToken = positioned {
    "'" ^^ (_ => QUOTE())
  }
}
