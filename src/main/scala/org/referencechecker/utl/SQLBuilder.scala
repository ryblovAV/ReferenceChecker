package org.referencechecker.utl

import org.referencechecker.models.TableKey

object SQLBuilder {

  val sqlMaster =
    s"""|select distinct t.referenced_name as table_name,
        |       (select cc.column_name
        |          from all_cons_columns cc, all_constraints c
        |         where cc.table_name = t.referenced_name
        |           and cc.owner = t.referenced_owner
        |           and c.constraint_name = cc.constraint_name
        |           and c.owner = cc.owner
        |           and c.constraint_type = 'P'
        |           and exists
        |                (select *
        |                   from all_tables k
        |                  where k.table_name = t.referenced_name || '_K'
        |                     and k.owner = t.referenced_owner)) as pk_column
        |          from dba_dependencies t
        |         where t.owner = 'LESKDATA'
        |           and t.type = 'PACKAGE BODY'
        |           and t.referenced_owner = 'STGADM'
        |           and t.referenced_type = 'TABLE'""".stripMargin

  def sqlDetails(columnName:String) =
    s"""|select t2.table_name, t2.column_name
        |  from all_tab_columns t2
        | where (t2.column_name like '%$columnName' or t2.column_name like '$columnName%')
        |   and t2.owner = 'STGADM'
        |   and t2.table_name like 'CI_%'
        |   and exists (select *
        |                 from dba_dependencies t
        |                where t.owner = 'LESKDATA'
        |                  and t.type = 'PACKAGE BODY'
        |                  and t.referenced_owner = t2.owner
        |                  and t.referenced_type = 'TABLE'
        |                  and t.referenced_name = t2.table_name)
        |   and t2.table_name != :table_name
        |   and not (t2.column_name != '$columnName'
        |        and exists (select *
        |                      from all_constraints c,
        |                           all_cons_columns cc
        |                     where c.constraint_name = cc.constraint_name
        |                       and c.owner = cc.owner
        |                       and c.owner = 'STGADM'
        |                       and c.constraint_type = 'P'
        |                       and cc.column_name = t2.column_name
        |                       and c.table_name like '%_K')
        |           )""".stripMargin

  def sqlCalcCountRow(tableName:String, owner:String = "STGADM") =
    s"""
       |select count(*)
       |  from $owner.$tableName""".stripMargin

  def buildSQL(mTable: String, mColumn: String, dTable: String, dColumn: String, owner: String = "STGADM"):String = {
    s"""
       |select count(*)
       |  from $owner.$dTable d
       | where not exists (select *
       |                     from $owner.$mTable m
       |                    where m.$mColumn = d.$dColumn)
       |   and d.$dColumn != ' '""".stripMargin
  }

  def buildSQLExistsForTable(mColumn: String, dTables: List[TableKey], owner: String) = {
    dTables.foldLeft(" ")((sql,tk) =>
      s"""|$sql
          |${if (sql != " ") "union all " else ""}
          |select d.${tk.column.getOrElse(mColumn)}
          |   from $owner.${tk.tableName} d
          |  where d.${tk.column.getOrElse(mColumn)} = m.$mColumn""".stripMargin)
  }

  def buildSQLBackwardCheck(mTable: String, mColumn: String, dTables: List[TableKey], owner: String = "STGADM") = {
    s"""
       |select count(*)
       |  from $owner.$mTable m
       | where not exists (
       |  ${buildSQLExistsForTable(mColumn, dTables, owner)}
       |  )
     """.stripMargin
  }


  def buildSQL(master: TableKey,detail: TableKey):String =
    buildSQL(master.tableName,master.column.get,detail.tableName,detail.column.getOrElse(master.column.get))

}
