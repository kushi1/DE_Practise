package mounikatest1.LTI_DE
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.util.{ Try, Success, Failure }
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


object storageMethods {
  
  def readCSV(path: String, delimiter: String = ",", header: String = "true"): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    import org.apache.spark.sql.functions._

    var dataFrame: org.apache.spark.sql.DataFrame = null
    Try {
      if (delimiter.length == 1) {
        dataFrame = spark.read.
          option("header", header).
          option("delimiter", delimiter).
          option("ignoreLeadingWhiteSpace", "true").
          option("ignoreTrailingWhiteSpace", "true").
          option("mode","DROPMALFORMED").
          //option("inferSchema", "true").
          csv(path)
      } else {
        var delimFormattedString = "\\"
        delimFormattedString += delimiter.mkString("\\")
        val rdd = spark.sparkContext.textFile(path)
        val columnHeader = rdd.first()
        val filteredRdd = rdd.filter(line => !line.equals(columnHeader))
        val columnHeadings = columnHeader.split(delimFormattedString)
        dataFrame = filteredRdd.map(line => line.split(delimFormattedString)).toDF.select((0 until columnHeadings.length).map(i => trim(col("value").getItem(i)).as(columnHeadings(i))): _*)
        
        
        
      }
    } match {
      case Success(obj) => dataFrame
      case Failure(obj) => {
        
        println(obj.getMessage() + " : " + obj.getCause())
        null
      }
    }
  }
  
  def saveAsCSV(dataFrameToSave: DataFrame, savePath: String, saveMode: SaveMode = SaveMode.Overwrite, delimiter: String = ",") {
    Try {
      dataFrameToSave.coalesce(1).write.mode(saveMode).
        option("header", "true").
        option("delimiter", delimiter).
        csv(savePath.toLowerCase())
    } match {
      case Success(obj) => {}
      case Failure(obj) => {
       
        obj.getMessage()
      }
    }
  }
  
}