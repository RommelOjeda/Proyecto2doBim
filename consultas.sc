import java.io.File

import kantan.csv._
import kantan.csv.ops._
//import java.nio.charset.CodingErrorAction
import scala.io.Codec

implicit val codec:Codec = Codec.ISO8859
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
                      estrasientos: String
                    )

val path2DataFile = "C:\\Users\\LENOVO\\Desktop\\PROYECTO\\datos2008.csv"
val dataSource = new File(path2DataFile)
  .readCsv[List, Matricula](rfc.withHeader())
val rows = dataSource.filter(row => row.isRight)
val values = rows.collect({ case Right(matricula) => matricula })
/*
val clasesV = values.map(_.clase).distinct.sorted
clasesV.foreach(println)
val servicios = values.map(_.servicio).distinct.sorted
servicios.foreach(println)
*/

//Numero de CAMIONETAS que tiene el MUNICIPIO,con sus MARCAS, clasificados por PROVINCIA
val autom1 = values.filter(row =>
  row.servicio == "MUNNICIPIO" &&  row.clase=="Camioneta")
  .groupBy(row => (row.provincia, row.marca))
  .map({ case ((prov, marc), v) => (prov, marc, v.length)})
autom1.foreach(data => printf("%s, %s, %d\n", data._1, data._2, data._3))

//Numero de AUTOMÓVILES matriculados entre 2005 y 2006, clasificados por MARCA
val autos2005 = values.filter(row => row.clase.startsWith("Autom") &&
  row.modelo == "2005"|row.modelo == "2006")
  .map(row => row.marca).groupBy(identity).map({case (x , y)=> (x,y.size)})
autos2005.foreach(data => printf("%s,%s\n",data._1,data._2))

//Reparto de los vehiculos segun el modelo que tienen
val modelo = values.map(row => (row.modelo, row.clase))
  .groupBy(identity).map({case (x , y)=> (x,y.length)})
modelo.foreach(data => printf("%s,%s\n",data._1,data._2))

//¿Cuántas camionetas tiene el estado y cuáles son sus marcas, clasificadas por provincia?
val camionetas = values
  .filter(row =>
    row.servicio == "ESTADO" && row.clase == "Camioneta")
  .groupBy(row => (row.provincia, row.marca))
  .map({ case ((prov, marca), v) => (prov, marca, v.length)})
camionetas.foreach(data => printf("%s, %s, %d\n", data._1, data._2, data._3))

//¿Cuántos Tanqueros usan diesel y cuáles son sus marcas, clasificadas por servicio?
val dieselTanquero = values
  .filter(row =>
    row.combustible == "Diesel" && row.clase == "Tanquero")
  .groupBy(row => (row.servicio, row.marca))
  .map({ case ((serv, marca), v) => (serv, marca, v.length)})
dieselTanquero.foreach(data => printf("%s, %s, %d\n", data._1, data._2, data._3))

//¿Cuántos BMW's son particulares y cual es su clase, clasificadas por provincia?
val bmwParticular = values
  .filter(row =>
    row.marca == "BMW" && row.servicio == "PARTICULAR")
  .groupBy(row => (row.provincia, row.clase))
  .map({ case ((prov, clase), v) => (prov, clase, v.length)})
bmwParticular.foreach(data => printf("%s, %s, %d\n", data._1, data._2, data._3))

new File("C:\\Users\\LENOVO\\Desktop\\Proyecto2doBim\\data\\modelo.csv")
  .writeCsv[(String,String,Int)](
    modelo.map(row => (row._1._1,row._1._2,row._2)), rfc
      .withHeader("modelo","clase","cantidad")
  )

new File("C:\\Users\\LENOVO\\Desktop\\Proyecto2doBim\\data\\autos2005.csv")
  .writeCsv[(String,Int)](
    autos2005.map(row => (row._1,row._2)), rfc
      .withHeader("marca","cantidad")
  )

new File("C:\\Users\\LENOVO\\Desktop\\Proyecto2doBim\\data\\autom1.csv")
  .writeCsv[(String,String,Int)](
    autom1.map(row => (row._1,row._2,row._3)), rfc
      .withHeader("provincia","marca","cantidad")
  )

new File("C:\\Users\\LENOVO\\Desktop\\Proyecto2doBim\\data\\camionetas.csv")
  .writeCsv[(String,String,Int)](
    camionetas.map(row => (row._1,row._2,row._3)), rfc
      .withHeader("provincia","marca","cantidad")
  )

new File("C:\\Users\\LENOVO\\Desktop\\Proyecto2doBim\\data\\dieselTanquero.csv")
  .writeCsv[(String,String,Int)](
    dieselTanquero.map(row => (row._1,row._2,row._3)), rfc
      .withHeader("servicio","marca","cantidad")
  )

new File("C:\\Users\\LENOVO\\Desktop\\Proyecto2doBim\\data\\bmwParticular.csv")
  .writeCsv[(String,String,Int)](
    bmwParticular.map(row => (row._1,row._2,row._3)), rfc
      .withHeader("provincia","clase","cantidad")
  )
