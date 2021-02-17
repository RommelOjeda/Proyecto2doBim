package pi.loaderdata

import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._


import java.io.File
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Codec

object UtilBrand {
  implicit val codec: Codec = Codec.ISO8859
  val path2DataFile = "C:\\Users\\LENOVO\\Desktop\\PROYECTO\\datos2008.csv"

  //Representa a la fila
  case class Brand(name: String, id: Long=0L)

  //Representa a la tabla
  class BrandTable(tag: Tag) extends Table[Brand](tag, "BRAND") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Brand]
  }

  //Medio para la ejecución de consultas
  val qryBrand = TableQuery[BrandTable]

  //Conexión con la base de datos
  val db = Database.forConfig("test01")

  def main(args: Array[String]): Unit = {
    //1. Crear esquema de datos, una única vez
    createDataSchema()
    //2. Cargar datos desde el archivo
    val brandList = loadDataFromFile(path2DataFile)
    //3. Importar datos a la base de datos
    importToDataBase(brandList)
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
    values.map(_.marca).distinct.sorted
  }

  def createDataSchema(): Unit = {
    println("Crear el esquema de datos")
    println(qryBrand.schema.createStatements.mkString)
    exec(qryBrand.schema.create)
  }

  def importToDataBase(brandList: List[String]): Unit = {
    val types: List[Brand] = brandList.map(Brand(_))
    println("Agregando datos")
    exec(qryBrand++= types)

  }

  /*def BrandQueries(): Unit = {

  }*/

  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), Duration.Inf)

}
