package pi.loaderdata

import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._


import java.io.File
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Codec

object UtilService {
  implicit val codec: Codec = Codec.ISO8859
  val path2DataFile = "C:\\Users\\LENOVO\\Desktop\\PROYECTO\\datos2008.csv"

  //Representa a la fila
  case class Service(name: String, id: Long=0L)

  //Representa a la tabla
  class ServiceTable(tag: Tag) extends Table[Service](tag, "SERVICE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Service]
  }

  //Medio para la ejecución de consultas
  val qryService = TableQuery[ServiceTable]

  //Conexión con la base de datos
  val db = Database.forConfig("test01")

  def main(args: Array[String]): Unit = {
    //1. Crear esquema de datos, una única vez
    createDataSchema()
    //2. Cargar datos desde el archivo
    val serviceList = loadDataFromFile(path2DataFile)
    //3. Importar datos a la base de datos
    importToDataBase(serviceList)
    //4. Realizar consultas
    //BrandQueries()
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

    //Traer la data del marca
    values.map(_.servicio).distinct.sorted
  }

  def createDataSchema(): Unit = {
    println("Crear el esquema de datos")
    println(qryService.schema.createStatements.mkString)
    exec(qryService.schema.create)
  }

  def importToDataBase(serviceList: List[String]): Unit = {
    val types: List[Service] = serviceList.map(Service(_))
    println("Agregando datos")
    exec(qryService++= types)

  }

  /*def BrandQueries(): Unit = {

  }*/

  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), Duration.Inf)

}
