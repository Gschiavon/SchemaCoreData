package FileReader

import java.io.{BufferedWriter, File, FileWriter}
import java.util.{Calendar, Date}
import java.text.DateFormat
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD


case class Utils(sparkContext: SparkContext) (implicit conf: Config) extends Serializable{

  def read(resource: String): RDD[String] = {
    val rdd = sparkContext.textFile(resource)
    val delimiter = conf.getString("csv.delimiter")
    val tableName = conf.getString("database.table.name")
    val now = getTime

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
        .map(line => line.split(delimiter, -1).toList)
        .map(lines => {
          lines(2) match {
            case "DATE" => s"$tableName;${lines(0)};${lines(1)};$now;dd/mm/yyyy;string;Falso;Falso;${lines(2)};$tableName\n"
            case "VARCHAR" => s"$tableName;${lines(0)};${lines(1)};$now;;string;Falso;Falso;${lines(2)}(${lines(3)});$tableName\n"
            case _ => s"$tableName;${lines(0)};${lines(1)};$now;;string;Falso;Falso;${lines(2)};$tableName\n"
          }
        })
    data
  }

  def writeToFile(rdd: RDD[String], filePath: String): Unit = {
    val lines = rdd.collect
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    val header = conf.getString("csv.header")
    val bw2 = addHeader(header, bw)

    for(value <- lines){
      bw2.write(value)
    }

    bw.close
    bw2.close
  }

  def addHeader(header: String, file: BufferedWriter): BufferedWriter = {
    file.write(s"$header")
    file.newLine()
    file
  }

  def getTime: String = {
    val currentDateTime = System.currentTimeMillis
    val currenDate = new Date(currentDateTime)
    val dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm:ss")
    dateFormat.format(currenDate)
  }
}
