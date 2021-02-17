import java.io.File
import kantan.csv._
import kantan.csv.ops._

import java.nio.charset.Charset

//import java.nio.charset.CodingErrorAction
//Add for class case use
import kantan.csv.generic._

import scala.collection.immutable.ListMap
import scala.io.Codec

implicit val codec:Codec = Codec.ISO8859
//codec.onMalformedInput(CodingErrorAction.REPLACE)
//codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

val glpText = new String("Gas liquado de petróleo".getBytes("UTF-8"), "ISO-8859-1")
println(glpText)
println("Gas liquado de petr�leo")

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

val path2DataFile = "/Users/jorgaf/Documents/Clases/Oct2020-Feb2021/Presencial/ProgramacionFuncional/DataProyecto/datos2008.csv"
//val dataSource = new File(path2DataFile).readCsv[List, (String, String, String, String, String, String, Double, Int, String, String)](rfc.withHeader())
val dataSource = new File(path2DataFile)
  .readCsv[List, Matricula](rfc.withHeader())
//dataSource.take(15).foreach(println)
val rows = dataSource.filter(row => row.isRight)
println(rows.size)

/*val errors = dataSource.filter(row => row.isLeft)
printf("Errors: %d", errors.size)*/

//val values = rows.collect({ case Right(t10) => t10 })
val values = rows.collect({ case Right(matricula) => matricula })
//values.take(15).foreach(println)

//¿Cuántas y cuáles son las clases de vehículos que existen en los registros?
//val clasesV = values.map(_._2).distinct.sorted
/*
val clasesV = values.map(_.clase).distinct.sorted
clasesV.foreach(println)
*/

//¿Cuántas y cuáles son los tipos de combustibles que existen en los registros?
//val combustibleClases = values.map(_._3).distinct.sorted
/*val combustibleClases = values.map(_.combustible).distinct.sorted
printf("%d clases de combustible\n", combustibleClases.size)
combustibleClases.foreach(println)*/

//¿Cuántas y cuáles son las marcas que existen en los registros?
//val marcas = values.map(_._4).distinct.sorted
/*val marcas = values.map(_.marca).distinct.sorted
printf("%d marcas\n", marcas.size)
marcas.foreach(println)*/

//¿Cuántas y cuáles son los tipos de servicio que existen en los registros?
//val servicios = values.map(_._5).distinct.sorted
/*
val servicios = values.map(_.servicio).distinct.sorted
printf("%d tipos de servicios\n", servicios.size)
servicios.foreach(println)*/

//¿Cuál es la clase de vehículo más popular que prestan servicio en el estado?
//val claseVehiculoEstado = values.filter(_._5 == "ESTADO").map(_._2)
/*val claseVehiculoEstado = values.filter(_.servicio == "ESTADO").map(_.clase)
claseVehiculoEstado.groupBy(identity)
claseVehiculoEstado.groupBy(identity).map({ case (k, v) => (k, v.length)})
ListMap(claseVehiculoEstado.groupBy(identity).map({ case (k, v) => (k, v.length)}).toSeq.sortBy(_._2):_*)
printf("La clase de vehículo más popular que presentan servicio dentro del estado es: %s", ListMap(claseVehiculoEstado.groupBy(identity).map({ case (k, v) => (k, v.length)}).toSeq.sortWith(_._2 > _._2):_*).head._1)
*/
//Case class

//¿Cuántas motocicletas tiene el estado y cuáles son sus marcas, clasificadas por provincia?
val motocicletas = values
  .filter(row =>
    row.servicio == "ESTADO" && row.clase == "Motocicleta")
  .groupBy(row => (row.provincia, row.marca))
  .map({ case ((prov, marca), v) => (prov, marca, v.length)})
motocicletas.foreach(data => printf("%s, %s, %d\n", data._1, data._2, data._3))


//¿Cuáles son las marcas, clases y el servicio que prestan los vehículos que usan Gas liquado de petróleo?
val dataGLP = values
.filter(row => row.combustible.contentEquals("Gas liquado de petróleo"))
  //.filter(row => row.combustible.equals("Gas liquado de petr�leo"))
  .map(row => (row.marca, row.clase, row.servicio))
  .groupBy(identity)
  .map({ case((marca, clase, servicio), lista) => ((marca, clase, servicio), lista.length)})

val dataGLPSorted = ListMap(dataGLP.toSeq.sortWith(_._2 > _._2):_*)

dataGLPSorted.foreach(row => printf("%s, %s, %s, %d\n",
  row._1._1,
  row._1._2,
  row._1._3,
  row._2))

/*new File("/Users/jorgaf/dataglp.csv")
  .writeCsv[(String, String, String, Int)](
    dataGLPSorted.map(row => (row._1._1, row._1._2, row._1._3, row._2)),
    rfc.withHeader("marca", "clase", "servicio", "cantidad")
  )

 */
values.map(_.combustible).filter(_.startsWith("Gas l")).map(_.getBytes(Charset.forName("UTF-8")))