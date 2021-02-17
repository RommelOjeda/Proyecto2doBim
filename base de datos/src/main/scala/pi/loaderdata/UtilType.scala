package pi.loaderdata

import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._


import java.io.File
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Codec

object UtilType {
  implicit val codec: Codec = Codec.ISO8859
  val path2DataFile = "C:\\Users\\LENOVO\\Desktop\\PROYECTO\\datos2008.csv"

  //Representa a la fila
  case class Type(name: String, id: Long=0L)

  //Representa a la tabla
  class TypeTable(tag: Tag) extends Table[Type](tag, "TYPE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Type]
  }

  //Medio para la ejecución de consultas
  val qryType = TableQuery[TypeTable]

  //Conexión con la base de datos
  val db = Database.forConfig("test01")

  def main(args: Array[String]): Unit = {
    //1. Crear esquema de datos, una única vez
    //createDataSchema()
    //2. Cargar datos desde el archivo
    //val typeList = loadDataFromFile(path2DataFile)
    //3. Importar datos a la base de datos
    //importToDataBase(typeList)
    //4. Realizar consultas
    typeQueries()
  }

  def loadDataFromFile(path2DataFile: String): List[String] = {
    case class Matricula(
                          provincia: String,
                          clase: String,
                          combustible: String,
                          marca: String,
                          servicio: String,
                          modelo: String,
                          tonelaje: Double,
                          asientos: Int,
                          estratone: String,
                          estrasientos: String)

    val dataSource = new File(path2DataFile).readCsv[List, Matricula](rfc.withHeader)
    val rows = dataSource.filter(row => row.isRight)
    val values = rows.collect({ case Right(matricula) => matricula })

    //Traer la data de la provincia
    values.map(_.clase).distinct.sorted
  }

  def createDataSchema(): Unit = {
    println("Crear el esquema de datos")
    println(qryType.schema.createStatements.mkString)
    exec(qryType.schema.create)
  }

  def importToDataBase(typeList: List[String]): Unit = {
    val types: List[Type] = typeList.map(Type(_))
    println("Agregando datos")
    exec(qryType ++= types)

  }

  def typeQueries(): Unit = {
    //A. Cargar todas las filas
    val clases = exec(qryType.result)

    //B. Cargar clases cuyo nombre inicia con una F
    val clasesStartL = qryType.filter(_.name.startsWith("f"))
    exec(clasesStartL.result).foreach(println)

    //C. El ID de la clase de Camioneta
    val camionetaID = qryType.filter(_.name === "Camioneta").map(_.id)
    println(exec(camionetaID.result).head)

  }

  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), 10.seconds)

}
