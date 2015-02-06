package org.referencechecker.utl

import java.sql.ResultSet

import org.referencechecker.models.{TableKey, Table}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.{JdbcTemplate, RowMapper}
import org.springframework.stereotype.Repository


@Repository("DBReader")
class JDBCUtl {

  @Autowired
  protected var jdbcTemplate:JdbcTemplate = _

  def getTables:List[Table] = {
    val rm = new RowMapper[Table] {
      override def mapRow(rs: ResultSet, rowNum: Int): Table = {
        Table(rs.getString(1),rs.getString(2),Nil)
      }
    }
    jdbcTemplate.query(SQLBuilder.sqlMaster,rm).toArray(Array[Table]()).toList
  }

  def getDetailsReferences(tableName:String, columnName:String):List[TableKey] = {
    val rm = new RowMapper[TableKey] {
      override def mapRow(rs: ResultSet, rowNum: Int): TableKey = {
        TableKey(rs.getString(1),Option(rs.getString(2)))
      }
    }
    jdbcTemplate.query(SQLBuilder.sqlDetails(columnName),rm,tableName).toArray(Array[TableKey]()).toList
  }

  def calcCount(sql:String):Int = {
    jdbcTemplate.queryForObject(sql,classOf[java.lang.Integer])
  }


  
}
