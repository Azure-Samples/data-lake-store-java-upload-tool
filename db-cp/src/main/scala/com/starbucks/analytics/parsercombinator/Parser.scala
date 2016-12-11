package com.starbucks.analytics.parsercombinator

import java.io.Reader

import com.starbucks.analytics.UploaderOptionsInfo
import com.starbucks.analytics.adls.ADLSConnectionInfo
import com.starbucks.analytics.db.DBConnectionInfo

import scala.util.parsing.combinator.RegexParsers

/**
 * Responsible for lexical & syntactic analysis of the internal DSL
 * used for uploading
 */
object Parser extends RegexParsers {
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

  // Combinators for lexing the language
  private def identifierToken: Parser[IDENTIFIER] = {
    "[a-zA-Z0-9_,\\/\\.]*".r ^^ { str => IDENTIFIER(str) }
  }

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

  private def assignmentToken: Parser[ASSIGNMENT] = positioned {
    ":=" ^^ (_ => ASSIGNMENT())
  }

  private def withToken: Parser[WITH] = positioned {
    "WITH" ^^ (_ => WITH())
  }

  private def usernameToken: Parser[USERNAME] = positioned {
    "USERNAME" ^^ (_ => USERNAME())
  }

  private def passwordToken: Parser[PASSWORD] = positioned {
    "PASSWORD" ^^ (_ => PASSWORD())
  }

  private def driverToken: Parser[DRIVER] = positioned {
    "DRIVER" ^^ (_ => DRIVER())
  }

  private def sourceToken: Parser[SOURCE] = positioned {
    "SOURCE" ^^ (_ => SOURCE())
  }

  private def clientIdToken: Parser[CLIENT_ID] = positioned {
    "CLIENTID" ^^ (_ => CLIENT_ID())
  }

  private def authToKenEndPointToken: Parser[AUTH_TOKEN_ENDPOINT] = positioned {
    "AUTHTOKENENDPOINT" ^^ (_ => AUTH_TOKEN_ENDPOINT())
  }

  private def clientKeyToken: Parser[CLIENT_KEY] = positioned {
    "CLIENTKEY" ^^ (_ => CLIENT_KEY())
  }

  private def targetToken: Parser[TARGET] = positioned {
    "TARGET" ^^ (_ => TARGET())
  }

  private def usingToken: Parser[USING] = positioned {
    "USING" ^^ (_ => USING())
  }

  private def selectToken: Parser[SELECT] = positioned {
    "SELECT FROM" ^^ (_ => SELECT())
  }

  private def ownerToken: Parser[OWNER] = positioned {
    "OWNER" ^^ (_ => OWNER())
  }

  private def tableToken: Parser[TABLES] = positioned {
    "TABLES" ^^ (_ => TABLES())
  }

  private def partitionsToken: Parser[PARTITIONS] = positioned {
    "PARTITIONS" ^^ (_ => PARTITIONS())
  }

  private def subPartitionsToken: Parser[SUB_PARTITIONS] = positioned {
    "SUBPARTITIONS" ^^ (_ => SUB_PARTITIONS())
  }

  private def predicateToken: Parser[PREDICATE] = positioned {
    "PREDICATE" ^^ (_ => PREDICATE())
  }

  private def equalsToken: Parser[EQUALS] = positioned {
    "=" ^^ (_ => EQUALS())
  }

  private def uploadToken: Parser[UPLOAD] = positioned {
    "UPLOAD TO" ^^ (_ => UPLOAD())
  }

  private def optionsToken: Parser[OPTIONS] = positioned {
    "OPTIONS" ^^ (_ => OPTIONS())
  }

  private def desiredBufferSizeToken: Parser[DESIRED_BUFFER_SIZE] = positioned {
    "DESIREDBUFFERSIZE" ^^ (_ => DESIRED_BUFFER_SIZE())
  }

  private def desiredParallelismToken: Parser[DESIRED_PARALLELISM] = positioned {
    "DESIREDPARALLELISM" ^^ (_ => DESIRED_PARALLELISM())
  }

  private def fetchSizeToken: Parser[FETCH_SIZE] = positioned {
    "FETCHSIZE" ^^ (_ => FETCH_SIZE())
  }

  private def separatorToken: Parser[SEPARATOR] = positioned {
    "SEPARATOR" ^^ (_ => SEPARATOR())
  }

  private def quoteToken: Parser[QUOTE] = positioned {
    "'" ^^ (_ => QUOTE())
  }

  // combinators for parsing declaration tokens
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

  def declaration: Parser[(String, Token)] = {
    variableToken ~ assignmentToken ~
      (interpolationToken | sqlToken) ^^ {
        case x ~ y ~ z => (x.str, z)
      }
  }

  def declarations: Parser[Option[Map[String, Token]]] = {
    opt(rep1(declaration)) ^^ { decl =>
      {
        if (decl.isDefined)
          Some(decl.get.map(x => x._1 -> x._2).toMap)
        else
          None
      }
    }
  }

  // combinators for parsing setup tokens
  private def username = usernameToken ~ literalToken
  private def password = passwordToken ~ literalToken
  private def driver = driverToken ~ literalToken
  private def source = sourceToken ~ literalToken
  private def clientId = clientIdToken ~ literalToken
  private def authTokenEndPoint = authToKenEndPointToken ~ literalToken
  private def clientKey = clientKeyToken ~ literalToken
  private def target = targetToken ~ literalToken
  private def setup: Parser[(DBConnectionInfo, ADLSConnectionInfo)] = {
    withToken ~> username ~ password ~ driver ~ source ~
      clientId ~ authTokenEndPoint ~ clientKey ~ target ^^ {
        case u ~ p ~ d ~ s ~ c ~ a ~ k ~ t => {
          val dbConnectionInfo = DBConnectionInfo(
            d._2.str,
            s._2.str,
            u._2.str,
            p._2.str
          )
          val adlsConnectionInfo = ADLSConnectionInfo(
            c._2.str,
            k._2.str,
            a._2.str,
            t._2.str
          )
          (dbConnectionInfo, adlsConnectionInfo)
        }
      }
  }

  // combinators for parsing select tokens
  private def owner = ownerToken ~ identifierToken
  private def table = tableToken ~ identifierToken
  private def partition = partitionsToken ~ identifierToken
  private def subPartition = subPartitionsToken ~ identifierToken
  private def predicate = predicateToken ~ identifierToken ~
    equalsToken ~ opt(quoteToken) ~ variableToken ~ opt(quoteToken)
  private def select = {
    selectToken ~> owner ~ table ~ opt(partition) ~
      opt(subPartition) ~ opt(predicate) ^^ {
        case o ~ t ~ p ~ s ~ pr => {
          val partitionList: Option[List[String]] = {
            if (p.isDefined)
              Some(p.get._2.str.split(",").toList)
            else
              None
          }
          val subPartitionList: Option[List[String]] = {
            if (s.isDefined)
              Some(s.get._2.str.split(",").toList)
            else
              None
          }

          (
            o._2.str,
            t._2.str.split(",").toList,
            partitionList,
            subPartitionList,
            pr
          )
        }
      }
  }

  // combinators for parsing target tokens
  private def targetPath: Parser[Token] = {
    uploadToken ~> (interpolationToken | identifierToken) ^^ (x => x)
  }

  // combinators for parsing options token
  private def desiredBufferSize = desiredBufferSizeToken ~ literalToken
  private def desiredParallelism = desiredParallelismToken ~ literalToken
  private def fetchSize = fetchSizeToken ~ literalToken
  private def separator = separatorToken ~ literalToken
  private def options: Parser[UploaderOptionsInfo] = {
    optionsToken ~> desiredBufferSize ~
      desiredParallelism ~ fetchSize ~ separator ^^ {
        case b ~ p ~ f ~ s => {
          UploaderOptionsInfo(
            b._2.str.toInt * 1024 * 1024,
            p._2.str.toInt,
            f._2.str.toInt,
            s._2.str.charAt(0)
          )
        }
      }
  }

  // Combinator that brings it all together
  private def block = {
    declarations ~ setup ~ select ~ targetPath ~ options ^^ {
      case d ~ s ~ sl ~ t ~ o => {
        // setup the declarations
        var declarationMap: Map[String, Token] = Map[String, Token]()
        if (d.isDefined)
          declarationMap = d.get

        // setup information
        val dbConnectionInfo = s._1
        val adlsConnectionInfp = s._2

        // get the target path and make the necessary substitutions
        val targetPath = new StringBuilder("s"
      }
    }
  }
}
