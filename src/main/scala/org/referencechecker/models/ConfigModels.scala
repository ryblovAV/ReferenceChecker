package org.referencechecker.models

case class TableKey(table:String,column:Option[String])



case class Table(name:String,pKeyColumn:Option[String],detailsRefs:List[TableKey])