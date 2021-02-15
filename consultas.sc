import java.io.File
import kantan.csv._
import kantan.csv.ops._
import java.nio.charset.Charset
//import java.nio.charset.CodingErrorAction
import kantan.csv.generic._
import scala.collection.immutable.ListMap
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

val path2DataFile = "C:\\2008.csv"
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


new File("\\Users\\romme\\modelo.csv")
  .writeCsv[(String,String,Int)](
    modelo.map(row => (row._1._1,row._1._2,row._2)), rfc
      .withHeader("modelo","clase","cantidad")
  )

new File("\\Users\\romme\\autos2005.csv")
  .writeCsv[(String,Int)](
    autos2005.map(row => (row._1,row._2)), rfc
      .withHeader("marca","cantidad")
  )

new File("\\Users\\romme\\autom1.csv")
  .writeCsv[(String,String,Int)](
    autom1.map(row => (row._1,row._2,row._3)), rfc
      .withHeader("provincia","marca","cantidad")
  )
