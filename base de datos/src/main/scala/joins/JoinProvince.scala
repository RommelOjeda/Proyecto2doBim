package joins

import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._
import slick.jdbc.MySQLProfile.api._
import java.io.File

import joins.JoinProvince.registration
import pi.loaderdata.UtilProvince.{exec, qryProvince}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.io.Codec

object JoinProvince extends App {
  //Representa a la fila
  case class Province(name: String, id: Long=0L)

  //Representa a la tabla
  class ProvinceTable(tag: Tag) extends Table[Province](tag, "PROVINCE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Province]
  }

  case class Type(name: String, id: Long=0L)

  class TypeTable(tag: Tag) extends Table[Type](tag, "TYPE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Type]
  }

  case class Fuel(name: String, id: Long=0L)

  class FuelTable(tag: Tag) extends Table[Fuel](tag, "FUEL") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Fuel]
  }

  case class Brand(name: String, id: Long=0L)

  class BrandTable(tag: Tag) extends Table[Brand](tag, "BRAND") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Brand]
  }

  case class Service(name: String, id: Long=0L)

  class ServiceTable(tag: Tag) extends Table[Service](tag, "SERVICE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Service]
  }

  lazy val qryProvince = TableQuery[ProvinceTable]
  lazy val qryType = TableQuery[TypeTable]
  lazy val qryFuel = TableQuery[FuelTable]
  lazy val qryBrand = TableQuery[BrandTable]
  lazy val qryService = TableQuery[ServiceTable]

  case class VehicleRegistration(modelo: String, tonelaje: Double, asientos: Int, estratone: String, estrasientos: String, provinceId: Long, typeId: Long, fuelId: Long, brandId: Long, serviceId: Long, id: Long=0L)

  class VehicleRegistrationTable(tag: Tag) extends Table[VehicleRegistration](tag, "VEHICLE_REGISTRATION") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def modelo = column[String]("MODELO")
    def tonelaje = column[Double]("TONELAJE")
    def asientos = column[Int]("ASIENTOS")
    def estratone = column[String]("ESTRATONE")
    def estrasientos = column[String]("ESTRASIENTOS")
    def provinceId = column[Long]("ID_PROVINCE")
    def typeId = column[Long]("ID_TYPE")
    def fuelId = column[Long]("ID_FUEL")
    def brandId = column[Long]("ID_BRAND")
    def serviceId = column[Long]("ID_SERVICE")

    def provinceRegistration = foreignKey("fk_province", provinceId, qryProvince)(_.id)
    def typeRegistration = foreignKey("fk_type", typeId, qryType)(_.id)
    def fuelRegistration = foreignKey("fk_fuel", fuelId, qryFuel)(_.id)
    def brandRegistration = foreignKey("fk_brand", brandId, qryBrand)(_.id)
    def serviceRegistration = foreignKey("fk_service", serviceId, qryService)(_.id)
    def * = (modelo, tonelaje, asientos, estratone, estrasientos, provinceId, typeId, fuelId, brandId, serviceId, id).mapTo[VehicleRegistration]

  }
  lazy val registrationQry = TableQuery[VehicleRegistrationTable]

  val db = Database.forConfig("test01")
  def exec[T](program:DBIO[T]): T = Await.result(db.run(program), Duration.Inf)

  //println((qryProvince.schema ++ qryType.schema ++ qryFuel.schema ++ registrationQry.schema).createStatements.mkString)
  //exec(registrationQry.schema.create)

  //DISEÑAR E IMPLEMENTAR UN MECANISMO PARA AGREGAR FILAS A LA TABLA VEHICLE_REGISTRATION
  implicit val codec:Codec = Codec.ISO8859
  val path2DataFile = "C:\\Users\\LENOVO\\Desktop\\PROYECTO\\datos2008.csv"
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

  val registration = values.map(row => (row.modelo, row.tonelaje, row.asientos, row.estratone, row.estrasientos,
    row.provincia, row.clase, row.combustible, row.marca, row.servicio))
  registration.take(20).foreach(println)

  val provinceList = exec(qryProvince.result)
  val typeList = exec(qryType.result)
  val fuelList = exec(qryFuel.result)
  val brandList = exec(qryBrand.result)
  val serviceList = exec(qryService.result)
  //provinceList.foreach(println)
  def getProvinceIdByName(provName: String) =
    provinceList.filter(row => row.name == provName).head.id

  def getTypeIdByName(typeName: String) =
    typeList.filter(row => row.name == typeName).head.id

  def getFuelIdByName(fuelName: String) =
    fuelList.filter(row => row.name == fuelName).head.id

  def getBrandIdByName(brandName: String) =
    brandList.filter(row => row.name == brandName).head.id

  def getServiceIdByName(serviceName: String) =
    serviceList.filter(row => row.name == serviceName).head.id

  def getSeqToInsert() =
    registration.map(t => VehicleRegistration(t._1, t._2, t._3, t._4, t._5,
      getProvinceIdByName(t._6), getTypeIdByName(t._7), getFuelIdByName(t._8), getBrandIdByName(t._9), getServiceIdByName(t._10)))

  exec(registrationQry ++= getSeqToInsert())
}