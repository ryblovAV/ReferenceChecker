package org.referencechecker.app

import grizzled.slf4j.Logging
import org.referencechecker.models.{Table, TableKey}
import org.referencechecker.utl._

object ReferenceCheckerApp extends App with Logging {


  def printMessage(master:Table, detailTK:TableKey, count:(Int,String)):Unit =
    info(s"${master.name} -> ${detailTK.table} count = ${count._1}")
  
  def printMessage(master:Table,refs:List[(TableKey,(Int,String))]):Unit =
    refs.foreach((a:(TableKey,(Int,String))) => printMessage(master,a._1,a._2))

  def checkCount(r:(Int,String)):Boolean =
    r._1 > 0

  def checkReference = {
    val tables = DBReader.readFromDB.sortBy(_.name)
    val checkRes:List[(Table,List[(TableKey,(Int,String))])] =
      tables.map((t) => (t,ReferenceChecker.checkRefs(t).filter((a:(TableKey,(Int,String))) => checkCount(a._2))))

    checkRes.foreach((a) => printMessage(a._1,a._2))
  }

  def calcCountFfromDB = {
    DBReader.getTablesWithCount.filter(_._1.endsWith("_K")==false).sortBy(_._1).foreach(info(_))
  }

  info("start")
  calcCountFfromDB
  info("end")
}