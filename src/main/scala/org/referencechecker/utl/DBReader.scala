package org.referencechecker.utl

import grizzled.slf4j.Logging
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.referencechecker.models.{TableKey, Table}

object DBReader extends Logging {

  val ctx = new ClassPathXmlApplicationContext("application-context.xml")
  val jdbcUtl: JDBCUtl = ctx.getBean(classOf[JDBCUtl])

  def getDetails(m: Table) =
    m.pKeyColumn match {
      case Some(column) => jdbcUtl.getDetailsReferences(m.name, column)
      case _ => Nil
    }

  def readFromDB = {
    jdbcUtl.getTables.map((m) => m.copy(detailsRefs = getDetails(m)))
  }

  def checkRefs(mTable:String,mColumn:String,dTable:String,dColumn:String):(Int,String) = {
    val sql: String = SQLBuilder.buildSQL(mTable, mColumn, dTable, dColumn)
    (jdbcUtl.calcCount(sql),sql)
  }

  def getTablesWithCount:List[(String,Int)] = {
    jdbcUtl.getTables.map((t:Table) => (t.name,jdbcUtl.calcCount(SQLBuilder.sqlCalcCountRow(t.name))))
  }


}
