package fhj.swengb.homework.dbtool

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import fhj.swengb.Person._
import fhj.swengb.homework.dbtool.Article
import fhj.swengb.{Person, Students}

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * Example to connect to a database.
  *
  * Initializes the database, inserts example data and reads it again.
  *
  */
object Db {

  /**
    * A marker interface for datastructures which should be persisted to a jdbc database.
    *
    * @tparam T the type to be persisted / loaded
    */
  trait DbEntity[T] {

    /**
      * Recreates the table this entity is stored in
      *
      * @param stmt
      * @return
      */
    def reTable(stmt: Statement): Int

    /**
      * Saves given type to the database.
      *
      * @param c
      * @param t
      * @return
      */
    def toDb(c: Connection)(t: T): Int

    /**
      * Given the resultset, it fetches its rows and converts them into instances of T
      *
      * @param rs
      * @return
      */
    def fromDb(rs: ResultSet): List[T]

    /**
      * Queries the database
      *
      * @param con
      * @param query
      * @return
      */
    def query(con: Connection)(query: String): ResultSet = {
      con.createStatement().executeQuery(query)
    }

    /**
      * Sql code necessary to execute a drop table on the backing sql table
      *
      * @return
      */
    def dropTableSql: String

    /**
      * sql code for creating the entity backing table
      */
    def createTableSql: String

    /**
      * sql code for inserting an entity.
      */
    def insertSql: String

  }

  lazy val maybeConnection: Try[Connection] =
    Try(DriverManager.getConnection("jdbc:sqlite::memory:"))

}

case class Employee(firstName: String) extends Db.DbEntity[Employee] {

  def reTable(stmt: Statement): Int = 0

  def toDb(c: Connection)(t: Employee): Int = 0

  def fromDb(rs: ResultSet): List[Employee] = List()

  def dropTableSql: String = ""

  def createTableSql: String = ""

  def insertSql: String = ""

}


object DbTool {

  def main(args: Array[String]) {
    for {con <- Db.maybeConnection
         _ = Person.reTable(con.createStatement())
         _ = Students.sortedStudents.map(toDb(con)(_))
         s <- Person.fromDb(queryAll(con))} {
      println(s)
    }
  }

}

case class Article(firstName: String) extends Db.DbEntity[Article] {
  def toDb(c: Connection)(t: Article) : Int = ???
  def fromDb(rs: ResultSet): List[Article] = ???

  def reTable(stmt: Statement) : Int = {
    stmt.executeUpdate(dropTableSql)
    stmt.executeUpdate(createTableSql)
  }
  def dropTableSql: String = "drop table if exists article"
  def createTableSql: String = "create table article (githubUsername string, firstName string, secondName String, groupId integer)"
  def insertSql: String = "insert into article (githubUsername, firstName, secondName, groupId) VALUES (?, ?, ?, ?)"
}



case class Customer(cNr : Int, firstName: String, lastName : String) extends Db.DbEntity[Customer] {
  def toDb(c: Connection)(cu: Customer) : Int = {
    val pstmt = c.prepareStatement(insertSql)
    pstmt.setInt(1, cu.cNr)
    pstmt.setString(2, cu.firstName)
    pstmt.setString(3, cu.lastName)
    pstmt.executeUpdate()
  }

  def fromDb(rs: ResultSet): List[Customer] = {
    val lb : ListBuffer[Customer] = new ListBuffer[Customer]()
    while (rs.next()) lb.append(Customer(rs.getInt("cNr"), rs.getString("firstName"), rs.getString("lastName")))
    lb.toList
  }

  def reTable(stmt: Statement) : Int = {
    stmt.executeUpdate(dropTableSql)
    stmt.executeUpdate(createTableSql)
  }
  def dropTableSql: String = "drop table if exists customer"
  def createTableSql: String = "create table customer (cnr Int, firstName String, lastName String)"
  def insertSql: String = "insert into customer (cNr, firstName, lastName) VALUES (?, ?, ?, ?)"
}
