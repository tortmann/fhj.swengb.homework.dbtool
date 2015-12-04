package fhj.swengb.homework.dbtool

import java.net.URL
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.ResourceBundle
import javafx.application.Application
import javafx.fxml._
import javafx.scene.control.TextField
import javafx.scene.{Scene, Parent}
import javafx.stage.Stage
import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.util.control.NonFatal

object dbTool {
  def main(args: Array[String]) {
    Application.launch(classOf[dbTool], args: _*)
  }
}

class dbTool extends javafx.application.Application {

  val fxml = "/fhj.swengb.homework.dbtool/dbtool.fxml"
  val css = "/fhj.swengb.homework.dbtool/dbstyle.css"
  val loader = new FXMLLoader(getClass.getResource(fxml))

  def setSkin(stage: Stage, fxml: String, css: String): Boolean = {
    val scene = new Scene(loader.load[Parent]())
    stage.setScene(scene)
    stage.getScene.getStylesheets.clear()
    stage.getScene.getStylesheets.add(css)
  }

  override def start(stage: Stage): Unit =
    try {
      stage.setTitle("Fruit Store - DB_Tool")
      loader.load[Parent]() // side effect
      val scene = new Scene(loader.getRoot[Parent])
      stage.setScene(scene)
      stage.getScene.getStylesheets.add(css)
      stage.show()
     } catch {
      case NonFatal(e) => e.printStackTrace()
    }

}

class dbtoolController extends Initializable {

  override def initialize(location: URL, resources: ResourceBundle): Unit = {
  }
}

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

  val a1:Article = Article(1, "apple", 0.5)
  val a2:Article = Article(2, "banana", 0.8)

  val articles:Set[Article] = Set(a1,a2)

  val c1:Customer = Customer(1, "Maier", "Franz")
  val c2:Customer = Customer(2, "Huber", "Sepp")

  val customers:Set[Customer] = Set(c1,c2)

  val o1:Order = Order(100, 1, "2015-01-01 08:00:00")
  val o2:Order = Order(101, 1, "2015-01-02 09:00:00")

  val orders:Set[Order] = Set(o1,o2)

  val op1:OrderPosition = OrderPosition(100, 1, 1, "apple", 5, 0.5)
  val op2:OrderPosition = OrderPosition(100, 2, 2, "banana", 1, 0.8)

  val orderpositions:Set[OrderPosition] = Set(op1,op2)


  def main(args: Array[String]) = {
     for {con <- Db.maybeConnection
     _ = Article.reTable(con.createStatement())
     _ = articles.map(Article.toDb(con)(_))
     a <- Article.fromDb(Article.queryAll(con))} {
     println(a)
     }
    }
}

object Article extends Db.DbEntity[Article] {
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
  def queryAll(con: Connection): ResultSet = query(con)("select * from article")
}

case class Article(artnr : Int, description :  String, price : Double) extends Db.DbEntity[Article] {
  def toDb(c: Connection)(a: Article) : Int = 0
  def fromDb(rs: ResultSet): List[Article] = List()
  def dropTableSql: String = ""
  def createTableSql: String = ""
  def insertSql: String = ""
}



object Customer extends Db.DbEntity[Customer] {
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
  def queryAll(con: Connection): ResultSet = query(con)("select * from customer")
}

case class Customer(cnr : Int, firstname: String, lastname : String) extends Db.DbEntity[Customer] {
  def toDb(c: Connection)(cu: Customer) : Int = 0
  def fromDb(rs: ResultSet): List[Customer] = List()
  def dropTableSql: String = ""
  def createTableSql: String = ""
  def insertSql: String = ""
}



object OrderPosition extends Db.DbEntity[OrderPosition] {
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
  def queryAll(con: Connection): ResultSet = query(con)("select * from orderposition")
}

case class OrderPosition(ordnr : Int, pos :  Int, article : Int, text: String,amount : Int, price: Double) extends Db.DbEntity[OrderPosition] {
  def toDb(c: Connection)(op: OrderPosition) : Int = 0
  def fromDb(rs: ResultSet): List[OrderPosition] = List()
  def dropTableSql: String = ""
  def createTableSql: String = ""
  def insertSql: String = ""
}



object Order extends Db.DbEntity[Order] {
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
  def queryAll(con: Connection): ResultSet = query(con)("select * from order")

}

case class Order(ordnr : Int,kdnr : Int, date : String) extends Db.DbEntity[Order] {
  def toDb(c: Connection)(o: Order) : Int = 0
  def fromDb(rs: ResultSet): List[Order] = List()
  def dropTableSql: String = ""
  def createTableSql: String = ""
  def insertSql: String = ""
}