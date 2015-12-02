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
    def reTable(stmt: Statement): Int = {
      stmt.executeUpdate(dropTableSql)
      stmt.executeUpdate(createTableSql)
    }

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



case class Article(artnr : Int, description :  String, price : Double) extends Db.DbEntity[Article] {
  def toDb(c: Connection)(a: Article) : Int = {
    val pstmt = c.prepareStatement(insertSql)
    pstmt.setInt(1, a.artnr)
    pstmt.setString(2, a.description)
    pstmt.setDouble(3, a.price)
    pstmt.executeUpdate()
  }
  def fromDb(rs: ResultSet): List[Article] = {
    val lb : ListBuffer[Article] = new ListBuffer[Article]()
    while (rs.next()) lb.append(Article(rs.getInt("artr"), rs.getString("description"), rs.getDouble("price")))
    lb.toList
  }

  def dropTableSql: String = "drop table if exists article"
  def createTableSql: String = "create table article (artnr integer, description string, price double)"
  def insertSql: String = "insert into article (artnr, desc, price) VALUES (?, ?, ?)"

}

case class Customer(cnr : Int, firstname: String, lastname : String) extends Db.DbEntity[Customer] {
  def toDb(c: Connection)(cu: Customer) : Int = {
    val pstmt = c.prepareStatement(insertSql)
    pstmt.setInt(1, cu.cnr)
    pstmt.setString(2, cu.firstname)
    pstmt.setString(3, cu.lastname)
    pstmt.executeUpdate()
  }

  def fromDb(rs: ResultSet): List[Customer] = {
    val lb : ListBuffer[Customer] = new ListBuffer[Customer]()
    while (rs.next()) lb.append(Customer(rs.getInt("cnr"), rs.getString("firstname"), rs.getString("lastname")))
    lb.toList
  }

  def dropTableSql: String = "drop table if exists customer"
  def createTableSql: String = "create table customer (cnr integer, firstname string, lastname string)"
  def insertSql: String = "insert into customer (cnr, firstname, lastname) VALUES (?, ?, ?, ?)"
}



case class OrderPosition(ordnr : Int, pos :  Int, article : Int, text: String,amount : Int, price: Double) extends Db.DbEntity[OrderPosition] {
  def toDb(c: Connection)(op: OrderPosition) : Int = {
    val pstmt = c.prepareStatement(insertSql)
    pstmt.setInt(1, op.ordnr)
    pstmt.setInt(2, op.pos)
    pstmt.setInt(3, op.article)
    pstmt.setString(4, op.text)
    pstmt.setDouble(5, op.price)
    pstmt.executeUpdate()
  }

  def fromDb(rs: ResultSet): List[OrderPosition] = {
    val lb : ListBuffer[OrderPosition] = new ListBuffer[OrderPosition]()
    while (rs.next()) lb.append(OrderPosition(rs.getInt("ordnr"), rs.getInt("article"), rs.getInt("pos"), rs.getString("text"),
                                              rs.getInt("amount"), rs.getDouble("price")))
    lb.toList
  }

  def dropTableSql: String = "drop table if exists orderposition"
  def createTableSql: String = "create table orderposition (ordnr Integer, pos   Integer, article  Integer, text String,amount  Integer, price  Double)"
  def insertSql: String = "insert into orderposition (ordnr, pos, article, text, amount, price) VALUES (?, ?, ?, ?, ?, ?)"

}


case class Order(ordnr : Int,kdnr : Int, date : String) extends Db.DbEntity[Order] {
  def toDb(c: Connection)(o: Order) : Int = {
    val pstmt = c.prepareStatement(insertSql)
    pstmt.setInt(1, o.ordnr)
    pstmt.setInt(2, o.kdnr)
    pstmt.setString(3, o.date)
    pstmt.executeUpdate()
  }
  def fromDb(rs: ResultSet): List[Order] = {
    val lb : ListBuffer[Order] = new ListBuffer[Order]()
    while (rs.next()) lb.append(Order(rs.getInt("ordnr"), rs.getInt("kdnr"), rs.getString("date")))
    lb.toList
  }

  def dropTableSql: String = "drop table if exists Order"
  def createTableSql: String = "create table Order (ordnr  Integer,kdnr  Integer, date  timestring)"
  def insertSql: String = "insert into Order (ordnr, kdnr, date) VALUES (?, ?, ?)"

}