package pi.loaderdata

import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._


import java.io.File
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Codec

object UtilProvince {
  implicit val codec: Codec = Codec.ISO8859
  val path2DataFile = "C:\\Users\\LENOVO\\Desktop\\PROYECTO\\datos2008.csv"

  //Representa a la fila
  case class Province(name: String, id: Long=0L)

  //Representa a la tabla
  class ProvinceTable(tag: Tag) extends Table[Province](tag, "PROVINCE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Province]
  }

  //Medio para la ejecución de consultas
  val qryProvince = TableQuery[ProvinceTable]

  //Conexión con la base de datos
  val db = Database.forConfig("test01")

  def main(args: Array[String]): Unit = {
    //1. Crear esquema de datos, una única vez
    //createDataSchema()
    //2. Cargar datos desde el archivo
    //val provinceList = loadDataFromFile(path2DataFile)
    //3. Importar datos a la base de datos
    //importToDataBase(provinceList)
    //4. Realizar consultas
    provinceQueries()

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
    values.map(_.provincia).distinct.sorted
  }

  def createDataSchema(): Unit = {
    println("Crear el esquma de datos")
    println(qryProvince.schema.createStatements.mkString)
    exec(qryProvince.schema.create)
  }

  def importToDataBase(pronviceList: List[String]): Unit = {
    val provinces: List[Province] = pronviceList.map(Province(_))
    println("Agregando datos")
    exec(qryProvince ++= provinces)

  }

  def provinceQueries(): Unit = {
    //A. Cargar todas las filas
    //val provinces = exec(qryProvince.result)

    //B. Cargar provincias cuy nombre inicia con una L
    val provincesStartL = qryProvince.filter(_.name.startsWith("l"))
    exec(provincesStartL.result).foreach(println)
    //C:\Users\LENOVO\Desktop\Proyecto2doBim\provinceL.csv
    new File("C:\\Users\\LENOVO\\Desktop\\Proyecto2doBim\\provinceL.csv")
      .writeCsv[(String, Long)](
        exec(provincesStartL.result).map(row => (row.name, row.id)),
        rfc.withHeader("Provincia", "Id")
      )


    //C. El ID de la provincia de Bolívar
    //val bolivarID = qryProvince.filter(_.name === "Bolívar").map(_.id)
    //println(exec(bolivarID.result).head)

  }

  def exec[T](program: DBIO[T]): T = Await.result(db.run(program), 10.seconds)

}
