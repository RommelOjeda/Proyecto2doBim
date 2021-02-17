package joins

import slick.jdbc.MySQLProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

object CreateObject extends App {
  case class User(name: String, id: Long=0L)

  class UserTable(tag: Tag) extends Table[User](tag, "user") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")

    def * = (name, id).mapTo[User]
  }

  lazy val usersQry = TableQuery[UserTable]
  lazy val insertUser= usersQry returning usersQry.map(_.id)


  case class Message(senderId: Long, content: String, id: Long=0L)

  class MessageTable(tag: Tag) extends Table[Message](tag, "message") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def senderId = column[Long]("senderId")
    def content = column[String]("content")

    def sender = foreignKey("sender_fk", senderId, usersQry)(_.id)

    def * = (senderId, content, id).mapTo[Message]
  }

  lazy val messageQry = TableQuery[MessageTable]
  lazy val insertMessage = messageQry returning messageQry.map(_.id)

  def freshTestData(daveId: Long, halId: Long) = Seq(
    Message(daveId, "Hello, HAL. Do you read me, HAL?"),
    Message(halId, "Affirmative, Dave. I read you."),
    Message(daveId, "Open the pod bay doors, HAL."),
    Message(halId, "I'm sorry, Dave. I'm afraid I can't do that.")
  )

  val setup = for {
    //_ <- (usersQry.schema ++ messageQry.schema).create
    daveId <- insertUser += User("Dave")
    halId <- insertUser += User("Hal")
    rowsAdd <- messageQry ++= freshTestData(daveId, halId)
  } yield rowsAdd

  val db = Database.forConfig("test01")

  def exec[T](program:DBIO[T]): T = Await.result(db.run(program), 8.seconds)

  exec(setup)

  val monadicFor = for {
    msg <- messageQry
    usr <- msg.sender
  } yield (usr.name, msg.content)

  exec(monadicFor.result).foreach(println)

}
