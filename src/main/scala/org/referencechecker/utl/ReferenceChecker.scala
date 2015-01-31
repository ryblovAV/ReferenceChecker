package org.referencechecker.utl

import org.referencechecker.models.{TableKey, Table}

object ReferenceChecker {

  def checkRefs(table: Table):List[(TableKey,(Int,String))] = {

    table.pKeyColumn match {
      case Some(masterColumn) =>
        table.detailsRefs.filter(_.table.endsWith("_K") == false).map(
          (dr) => (dr,
            DBReader.checkRefs(table.name,
              masterColumn,
              dr.table,
              dr.column.getOrElse(masterColumn))))
      case None => Nil
    }
  }


}
