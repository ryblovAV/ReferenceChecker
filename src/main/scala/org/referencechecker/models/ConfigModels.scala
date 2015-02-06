package org.referencechecker.models

case class TableKey(tableName:String,column:Option[String])

case class Table(tableName:String,pKeyColumnName:String,detailsRefs:List[TableKey])