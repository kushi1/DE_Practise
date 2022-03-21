package mounikatest1.LTI_DE

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import mounikatest1.LTI_DE.storageMethods._
import org.apache.spark.sql.expressions.Window

object Problem1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("VideoEX").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
   /* Ass:1.filter records with date format other than dd-MM-yyyy
    * 2.columns are renamed 
    * 3.considering null records
*/    
    //No of input records media:71439  date null col: 41175 paid: 1001 and after cleansing media:30259 
    
    var media_DF=readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/Media_Campaigns_csv.csv")
   
  media_DF=media_DF.withColumn("Date",to_date('Date,"dd-mm-yyyy") ).where('Date.isNotNull)
    var paid_DF=readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/Paid_Search_csv.csv")
   
    //column rename
    val srcTgtMap = readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/srctotar.csv").rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap
   
   for (i <- srcTgtMap.keys) {
      media_DF = media_DF.withColumnRenamed(i, srcTgtMap(i))
    }
  val paid_srcTgtMap=readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/paid_srctotar.csv").rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap
  
   for (i <- paid_srcTgtMap.keys) {
      paid_DF = paid_DF.withColumnRenamed(i, paid_srcTgtMap(i))
    }
//1.VCR=videoviews/videocompletion
  media_DF=media_DF.where('video_completes.isNotNull)
 
var vcr_df=media_DF.withColumn("vcr",$"video_views"/$"video_completes").where('vcr.isNotNull).select($"partner",$"Campaign",$"vcr")
val windowSpec = Window.partitionBy('campaign).orderBy('vcr.desc)
vcr_df=vcr_df.withColumn("dummyCol",row_number().over(windowSpec)).drop("dummyCol").where('dummyCol<=5)
saveAsCSV(vcr_df,"C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/output/vcr1")


//2.VTR (Video Through Rate) [Hint: Video Views / Impressions] Video_Views
var vtr_df=media_DF.where('Impressions.isNotNull)
vtr_df=vtr_df.withColumn("vtr",$"video_views"/$"impressions").where('vtr.isNotNull).select($"partner",$"campaign",$"vtr")
val vtr_windowspec=Window.partitionBy('campaign).orderBy('vtr.desc)
vtr_df=vtr_df.withColumn("dummyCol",row_number().over(vtr_windowspec)).drop("dummyCol").where('dummyCol<=5)
saveAsCSV(vtr_df,"C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/output/vtr1")

  }
}