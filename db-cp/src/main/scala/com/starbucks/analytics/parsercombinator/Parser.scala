package com.starbucks.analytics.parsercombinator

import java.io.Reader

import com.google.inject.Guice
import com.starbucks.analytics._
import com.starbucks.analytics.adls.ADLSConnectionInfo
import com.starbucks.analytics.db.{ DBConnectionInfo, DBManager, SchemaInfo, SqlGenerator }
import com.starbucks.analytics.di.SqlGeneratorModule

import scala.collection.{ immutable, mutable }
import scala.language.postfixOps
import scala.util.Try
import scala.util.parsing.combinator.RegexParsers

/**
 * Responsible for lexical & syntactic analysis of the internal DSL
 * used for uploading
 */
object Parser extends RegexParsers {
  // Produces a cross product of two traversable
  // collections
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X, Option[Y])] = xs.flatMap(lhs => {
      if (ys.isEmpty)
        Seq((lhs, None))
      else
        ys.map(rhs => {
          (lhs, Some(rhs))
        })
    })
  }

  def parsePartitions(
    declarationMap:   mutable.Map[String, Token],
    dbConnectionInfo: DBConnectionInfo,
    token:            Token
  ): Option[List[String]] = {
    token match {
      case VARIABLE(str) =>
        if (declarationMap.contains(str)) {
          declarationMap(str) match {
            case INTERPOLATION(i) =>
              val result = InterpolationParser.parse(i, dbConnectionInfo, declarationMap)
              if (result.isRight) {
                return Some(result.right.get.split(",").toList)
              } else {
                return None
              }
            case SQL(s) =>
              val result = DBManager.withResultSetIterator[List[String], String](
                dbConnectionInfo,
                s,
                10, {
                  result => result.getString(1)
                }, {
                  resultSetIterator => resultSetIterator.toList
                }
              )
              if (result.isSuccess) {
                return Some(result.get)
              } else {
                throw new Exception(
                  s"""
                     |The variable $str is defined as a SQL Statement $s. Executing the
                     |SQL statement resulted in an exception ${result.failed.get}
                     """.stripMargin
                )
              }
            case unknown =>
              throw new Exception(s"The variable $str is defined but don't know how to parse $unknown.")
          }
        } else {
          throw new Exception(s"The variable $str is not declared.")
        }
      case _ =>
        None
    }
    None
  }

  /**
   * Parsers the input stream and presents the results
   * of lexing the content in the stream
   *
   * @param reader Input reader
   * @return Result of lexing
   */
  def parse(reader: Reader): Either[UploaderParserError, (DBConnectionInfo, ADLSConnectionInfo, UploaderOptionsInfo, Map[Option[(String, List[String])], Option[String]])] = {
    val parsed = parseAll(fileBlock, reader)
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

  // Combinators for lexing the language
  private def commentToken: Parser[COMMENT] = {
    """(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)""".stripMargin.r ^^ { str => COMMENT(str) }
  }

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

  private def functionToken: Parser[FUNCTION] = {
    "^`[a-zA-Z0-9\\-_,\\/\\.\\(\\)'\\s]*".r ^^ { str => FUNCTION(str.substring(1)) }
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

  private def operatorToken: Parser[OPERATOR] = positioned {
    ">=|<=|=|>|<".r ^^ (op => OPERATOR(op))
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

  private def rowSeparatorToken: Parser[ROWSEPARATOR] = positioned {
    "ROWSEPARATOR" ^^ (_ => ROWSEPARATOR())
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
        case x ~ _ ~ z => (x.str, z)
      }
  }

  def declarations: Parser[Option[mutable.Map[String, Token]]] = {
    opt(rep1(declaration)) ^^ { decl =>
      {
        if (decl.isDefined) {
          Some(mutable.Map(decl.get.map(x => (x._1, x._2)): _*))
        } else
          None
      }
    }
  }

  // combinators for parsing setup tokens
  private def username = usernameToken ~ literalToken
  private def password = passwordToken ~ (literalToken | variableToken)
  private def driver = driverToken ~ literalToken
  private def source = sourceToken ~ literalToken
  private def clientId = clientIdToken ~ literalToken
  private def authTokenEndPoint = authToKenEndPointToken ~ literalToken
  private def clientKey = clientKeyToken ~ literalToken
  private def target = targetToken ~ literalToken
  private def setup: Parser[(DBConnectionInfo, ADLSConnectionInfo)] = {
    withToken ~> username ~ password ~ driver ~ source ~
      clientId ~ authTokenEndPoint ~ clientKey ~ target ^^ {
        case u ~ p ~ d ~ s ~ c ~ a ~ k ~ t =>
          val dbConnectionInfo = DBConnectionInfo(
            driver = d._2.str,
            connectionStringUri = s._2.str,
            username = u._2.str,
            password = {
            p._2 match {
              case LITERAL(literal) =>
                literal
              // TODO: Add support for variables
              case _ =>
                ""
            }
          }
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

  // combinator for parsing the sql generator
  private def sqlGenerator = usingToken ~ identifierToken

  // combinators for parsing select tokens
  private def owner = ownerToken ~ identifierToken
  private def table = tableToken ~ identifierToken
  private def partition = partitionsToken ~> variableToken
  private def subPartition = subPartitionsToken ~> variableToken
  private def predicate = predicateToken ~ identifierToken ~ opt(functionToken) ~
    operatorToken ~ opt(quoteToken) ~ variableToken ~ opt(quoteToken)
  private def select = {
    selectToken ~> owner ~ table ~ opt(partition) ~
      opt(subPartition) ~ opt(predicate) ^^ {
        case o ~ t ~ p ~ s ~ pr =>
          (
            o._2.str,
            t._2.str.split(",").toList,
            p,
            s,
            pr
          )
      }
  }

  // combinators for parsing target tokens
  private def targetPath: Parser[Token] = {
    uploadToken ~> (variableToken | literalToken | interpolationToken) ^^ (x => x)
  }

  // combinators for parsing options token
  private def desiredBufferSize = desiredBufferSizeToken ~ literalToken
  private def desiredParallelism = desiredParallelismToken ~ literalToken
  private def fetchSize = fetchSizeToken ~ literalToken
  private def separator = separatorToken ~ literalToken
  private def rowSeparator = rowSeparatorToken ~ literalToken
  private def options: Parser[UploaderOptionsInfo] = {
    optionsToken ~> desiredBufferSize ~ desiredParallelism ~
      fetchSize ~ separator ~ rowSeparator ^^ {
        case b ~ p ~ f ~ s ~ rs =>
          UploaderOptionsInfo(
            b._2.str.toInt * 1024 * 1024,
            p._2.str.toInt,
            f._2.str.toInt,
            // Note: Support non-printable characters
            // Converting the string to integer and translating
            // to Char gives the Hex value. If not, use the
            // first character of the string passed in as
            // the parameter for separator
            {
              var sep: Char = 0x00
              val conv = Try(Integer.parseInt(s._2.str, 16))
              if (conv.isSuccess) {
                sep = conv.get.toChar
              } else {
                val m: Option[String] = "\t".r.findFirstIn(s._2.str)
                if (m.isDefined)
                  sep = '\t'
                else
                  sep = s._2.str.charAt(0)
              }
              sep
            },
            {
              var sep: Char = 0x00
              val conv = Try(Integer.parseInt(rs._2.str, 16))
              if (conv.isSuccess) {
                sep = conv.get.toChar
              } else {
                val m: Option[String] = "\n".r.findFirstIn(s._2.str)
                if (m.isDefined)
                  sep = '\n'
                else
                  sep = s._2.str.charAt(0)
              }
              sep
            }
          )
      }
  }

  // Combinator that brings it all together
  private def block = {
    declarations ~ setup ~ sqlGenerator ~ select ~ targetPath ~ options ^^ {
      case d ~ s ~ sg ~ sl ~ t ~ o =>
        var sqlStatements: Map[Option[(String, List[String])], Option[String]] =
          Map[Option[(String, List[String])], Option[String]]()

        // setup the declarations
        var declarationMap: mutable.Map[String, Token] = mutable.Map[String, Token]()
        if (d.isDefined)
          declarationMap = d.get

        // setup information
        val dbConnectionInfo = s._1
        val adlsConnectionInfo = s._2

        // parse the sql generator and dynamically inject the right provider
        val sqlStatementGenerator: SqlGenerator = sg match {
          case _ ~ id =>
            val injector = Guice.createInjector(new SqlGeneratorModule())
            import net.codingwell.scalaguice.InjectorExtensions._
            val map = injector.instance[immutable.Map[String, SqlGenerator]]
            if (map.contains(id.str.toUpperCase)) {
              map(id.str.toUpperCase)
            } else {
              throw new Exception(s"SQL Generator ${id.str.toUpperCase} is not defined")
            }
          case _ => throw new Exception(s"Unknown sql generator $sg")
        }

        // Parse the predicates
        var predicateList = List[String]()
        var isQuoted = false
        var predicateColumn = ""
        var predicateAlias = ""
        var operator = "="
        if (sl._5.isDefined) {
          sl._5.get match {
            case _ ~ id ~ fn ~ op ~ sq ~ v ~ eq =>
              // column Name
              predicateAlias = id.str
              // functions
              if (fn.isDefined) {
                predicateColumn = fn.get.fun
              } else {
                predicateColumn = predicateAlias
              }
              // operator
              operator = op.op
              // quotation
              if (sq.isDefined) {
                if (eq.isDefined) {
                  isQuoted = true
                } else {
                  throw new Exception("Imbalanced quote. Not able to parse predicates.")
                }
              }
              // predicate value
              if (declarationMap.contains(v.str)) {
                declarationMap(v.str) match {
                  case SQL(i) =>
                    val predicateResultSet = DBManager.withResultSetIterator[List[String], String](
                      dbConnectionInfo,
                      i,
                      o.fetchSize, {
                      result => result.getString(1)
                    }, {
                      resultSetIterator => resultSetIterator.toList
                    }
                    )
                    if (predicateResultSet.isSuccess) {
                      predicateList = predicateResultSet.get
                    } else {
                      throw new Exception(
                        s"""
                           |There was a problem executing $i. Unable to substitute predicates
                           |for column $predicateAlias using variable ${v.str}.
                         """.stripMargin
                      )
                    }
                  case LITERAL(lit) =>
                    predicateList = List(lit)
                  case _ => throw new Exception(s"Unknown variable ${v.str} is defined for predicate $predicateAlias")
                }
              } else {
                throw new Exception(s"Cannot find the variable definition ${v.str} for predicate $predicate")
              }
          }
        }

        // generate the schema information
        val partitions = if (sl._3.isDefined) {
          parsePartitions(declarationMap, dbConnectionInfo, sl._3.get)
        } else
          None
        val subPartitions = if (sl._4.isDefined)
          parsePartitions(declarationMap, dbConnectionInfo, sl._4.get)
        else
          None
        val schemaList = DBManager.withResultSetIterator[List[SchemaInfo], SchemaInfo](
          dbConnectionInfo,
          sqlStatementGenerator.getPartitions(
            sl._1,
            sl._2,
            partitions,
            subPartitions
          ),
          o.fetchSize, {
            resultSet =>
              SchemaInfo(
                resultSet.getString(1),
                resultSet.getString(2),
                Option(resultSet.getString(3)),
                Option(resultSet.getString(4))
              )
          }, {
            resultsIterator => resultsIterator.toList
          }
        )
        if (schemaList.isSuccess) {
          val mergedList = schemaList.get cross predicateList
          sqlStatements = mergedList.map((s) => {
            val schema = s._1
            val pred = s._2
            // Add system variables to the symbol/declaration map
            declarationMap("OWNER") = LITERAL(schema.owner.toLowerCase)
            declarationMap("TABLE") = LITERAL(schema.tableName.toLowerCase)
            declarationMap(predicateAlias) = pred match {
              case Some(pa) =>
                LITERAL(pa)
              case None =>
                EMPTY()
            }
            declarationMap("PARTITION") = {
              if (schema.partitionName.isDefined) LITERAL(schema.partitionName.get.toLowerCase) else EMPTY()
            }
            declarationMap("SUBPARTITION") = {
              if (schema.subPartitionName.isDefined) LITERAL(schema.subPartitionName.get.toLowerCase) else EMPTY()
            }

            val columnList: Try[List[String]] = DBManager.withResultSetIterator[List[String], String](
              dbConnectionInfo,
              sqlStatementGenerator.getColumnNames(
                schema.owner,
                schema.tableName
              ),
              o.fetchSize, {
                resultSet => resultSet.getString(1)
              }, {
                resultSetIterator => resultSetIterator.toList
              }
            )
            if (columnList.isSuccess) {
              Some((
                sqlStatementGenerator.getData(schema, columnList.get, {
                  if (pred.isDefined) {
                    val builder = new StringBuilder
                    builder ++= predicateColumn
                    builder ++= operator
                    if (isQuoted) {
                      builder ++= s"'${pred.get}'"
                    } else {
                      builder ++= pred.get
                    }
                    Some(builder.toString)
                  } else {
                    None
                  }
                }),
                columnList.get
              )) ->
                Some({
                  t match {
                    case LITERAL(str) =>
                      str
                    case VARIABLE(str) =>
                      if (declarationMap.contains(str)) {
                        declarationMap(str) match {
                          case INTERPOLATION(i) =>
                            val result = InterpolationParser.parse(i, dbConnectionInfo, declarationMap)
                            if (result.isRight)
                              result.right.get
                            else
                              "" // Should not happen
                          case unknown =>
                            throw new Exception(s"The variable $str is defined but don't know how to parse $unknown.")
                        }
                      } else {
                        throw new Exception(s"The variable $str is not declared.")
                      }
                    case _ =>
                      throw new Exception(s"Unrecognized token $t")
                  }
                })
            } else {
              None -> None
            }
          }).toMap
        }
        (dbConnectionInfo, adlsConnectionInfo, o, sqlStatements)
    }
  }

  private def fileBlock = {
    opt(commentToken) ~> block <~ opt(commentToken)
  }
}
