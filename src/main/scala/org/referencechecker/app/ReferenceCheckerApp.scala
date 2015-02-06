package org.referencechecker.app

import grizzled.slf4j.Logging
import org.referencechecker.models.{Table, TableKey}
import org.referencechecker.utl._

object ReferenceCheckerApp extends App with Logging {


  def printMessage(master: Table, detailTK: TableKey, countWithSql: CountWithSQL, printSQL: Boolean): Unit =
    info(s"${master.tableName}(${master.pKeyColumnName}}) -> ${detailTK.tableName}(${detailTK.column}}) count = ${countWithSql.count} ${if (printSQL) countWithSql.sql}")

  def printMessage(master: Table, refs: List[(TableKey, CountWithSQL)], printSQL: Boolean): Unit =
    refs.foreach((a: (TableKey, CountWithSQL)) => printMessage(master, a._1, a._2, printSQL))

  def printMessage(table: Table, countWithSQL: CountWithSQL, printSQL: Boolean): Unit =
    info(
      s"""|${table.tableName}(${table.pKeyColumnName})  count = ${countWithSQL.count}
          |${if (printSQL) countWithSQL.sql else "" }""".stripMargin)

  def createListTables: List[Table] =
    List(Table("CI_SA_SP", "SP_ID", List(TableKey("CI_SP", Option("SP_ID")))),
      Table("CI_MTR", "MTR_ID", List(TableKey("CI_MTR_CONFIG", Option("MTR_ID")))),
      Table("CI_SP_MTR_HIST", "MTR_CONFIG_ID", List(TableKey("CI_MTR_CONFIG", Option("MTR_CONFIG_ID")))))

  def createBackwardListTables: List[Table] =
    List(Table("CI_PREM", "PREM_ID",
      List(
        TableKey("CI_SP", Some("PREM_ID")),
        TableKey("CI_SA", Some("CHAR_PREM_ID")),
        TableKey("CI_ACCT", Some("MAILING_PREM_ID")))))


  def checkReference(tables: List[Table],
                     filterFunc: (CountWithSQL) => Boolean,
                     printSQL: Boolean = false) = {
    val checkRes: List[(Table, List[(TableKey, CountWithSQL)])] =
      tables.sortBy(_.tableName).map((t) => (t, ReferenceChecker.checkRefs(t).filter((a: (_, CountWithSQL)) => filterFunc(a._2))))

    checkRes.foreach((a) =>
      a match {
        case (table, detailsRefs) => printMessage(table, detailsRefs, printSQL)
        case _ => throw new Exception("error result check refs")
      }
    )
  }

  def checkBackwardReference(tables: List[Table],
                             filterFunc: (CountWithSQL) => Boolean,
                             printSQL: Boolean = false) = {
    val checkRes: List[(Table, CountWithSQL)] =
      tables.sortBy(_.tableName).map((t) => (t, ReferenceChecker.checkBackwardsRefs(t))).filter((a: (_, CountWithSQL)) => filterFunc(a._2))

    checkRes.foreach((a) => printMessage(a._1, a._2, printSQL))
  }

  def calcCountFfromDB = {
    DBReader.getTablesWithCount.filter((t) => (t._1.endsWith("_K") == false) && (t._2 > 0)).sortBy(_._1).foreach(info(_))
  }

  info("start")

  calcCountFfromDB
  info("-----Not find master --------------------------------------")
  checkReference(DBReader.readFromDB,(c) => c.count > 0)
  info("-----Not reference --------------------------------------")
  checkReference(createListTables,(c) => c.count > 0)
  info("-----Not reference (many Table) --------------------------------------")
  checkBackwardReference(createBackwardListTables, (c) => c.count > 0)
  info("end")
}