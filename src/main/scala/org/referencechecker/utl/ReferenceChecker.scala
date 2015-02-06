package org.referencechecker.utl

import org.referencechecker.models.{TableKey, Table}

case class CountWithSQL(count: Int, sql: String)

object ReferenceChecker {

  def checkRefs(table: Table): List[(TableKey, CountWithSQL)] = {
    table.detailsRefs.filter(_.tableName.endsWith("_K") == false).map(
      (dr) => (dr,
        DBReader.checkRefs(table.tableName,
          table.pKeyColumnName,
          dr.tableName,
          dr.column.getOrElse(table.pKeyColumnName))))
  }

  def checkBackwardsRefs(table: Table):CountWithSQL = {
    DBReader.checkBackwardRefs(table)
  }


}
