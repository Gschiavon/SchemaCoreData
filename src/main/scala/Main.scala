

import FileReader.Utils
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}


object Main extends Serializable {

  implicit val conf = ConfigFactory.load

  val appName = conf.getString("spark.appName")
  val master =  conf.getString("spark.master")
  val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(master))

  def main(args: Array[String]): Unit = {

    if(args.size >= 1) {

      val fileOutputPath = args(1)
      val utils = Utils(sc)
      val fileInputPath = args(0)
      val rdd = utils.read(fileInputPath)
      utils.writeToFile(rdd, fileOutputPath)
    }

  }
}
