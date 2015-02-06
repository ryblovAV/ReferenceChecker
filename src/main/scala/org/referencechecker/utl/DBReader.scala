package org.referencechecker.utl

import grizzled.slf4j.Logging
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.referencechecker.models.{TableKey, Table}

object DBReader extends Logging {

  val ctx = new ClassPathXmlApplicationContext("application-context.xml")
  val jdbcUtl: JDBCUtl = ctx.getBean(classOf[JDBCUtl])

  def getDetails(m: Table) =
      jdbcUtl.getDetailsReferences(m.tableName, m.pKeyColumnName)

  def readFromDB = {
    jdbcUtl.getTables.map((m) => m.copy(detailsRefs = getDetails(m)))
  }

  def checkRefs(mTable:String,mColumn:String,dTable:String,dColumn:String):CountWithSQL = {
    val sql: String = SQLBuilder.buildSQL(mTable, mColumn, dTable, dColumn)
    CountWithSQL(jdbcUtl.calcCount(sql),sql)
  }

  def getTablesWithCount:List[(String,Int)] = {
    jdbcUtl.getTables.map((t:Table) => (t.tableName,jdbcUtl.calcCount(SQLBuilder.sqlCalcCountRow(t.tableName))))
  }

  def checkBackwardRefs(table: Table) = {
    val sql: String = SQLBuilder.buildSQLBackwardCheck(table.tableName,table.pKeyColumnName,table.detailsRefs)
    CountWithSQL(jdbcUtl.calcCount(sql),sql)
  }

}
