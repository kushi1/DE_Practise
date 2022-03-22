package mounikatest1.LTI_DE

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import mounikatest1.LTI_DE.storageMethods._
import org.apache.spark.sql.expressions.Window

object Problem1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("VideoEX").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    /* Ass:1.filter records with date format other than dd-MM-yyyy
    * 2.columns are renamed
    * 3.considering null records
*/
    //No of input records media:71439  date null col: 41175 paid: 1001 and after cleansing media:30259

    var media_DF = readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/Media_Campaigns_csv.csv")

    media_DF = media_DF.withColumn("Date", from_unixtime(unix_timestamp('Date, "MM-dd-yyyy"), "dd-MM-yyyy")).where('Date.isNotNull)

    var paid_DF = readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/Paid_Search_csv.csv")

    //column rename
    val srcTgtMap = readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/srctotar.csv").rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap

    for (i <- srcTgtMap.keys) {
      media_DF = media_DF.withColumnRenamed(i, srcTgtMap(i))
    }
    val paid_srcTgtMap = readCSV("C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/A_Data_Engineering_Practice_and_Hands_on_Coding_Challenge/paid_srctotar.csv").rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap

    for (i <- paid_srcTgtMap.keys) {
      paid_DF = paid_DF.withColumnRenamed(i, paid_srcTgtMap(i))
    }
    //1.VCR=videoviews/videocompletion
    var vcr_df = media_DF.where('video_completes.isNotNull)

    vcr_df = vcr_df.withColumn("vcr", $"video_views" / $"video_completes").where('vcr.isNotNull).select($"partner", $"Campaign", $"vcr")
    val windowSpec = Window.partitionBy('campaign).orderBy('vcr.desc)
    vcr_df = vcr_df.withColumn("dummyCol", row_number().over(windowSpec)).drop("dummyCol").where('dummyCol <= 5)
    saveAsCSV(vcr_df,"C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/output/vcr1")

    //2.VTR (Video Through Rate) [Hint: Video Views / Impressions] Video_Views
    var vtr_df = media_DF.where('Impressions.isNotNull)
    vtr_df = vtr_df.withColumn("vtr", $"video_views" / $"impressions").where('vtr.isNotNull).select($"partner", $"campaign", $"vtr")
    val vtr_windowspec = Window.partitionBy('campaign).orderBy('vtr.desc)
    vtr_df = vtr_df.withColumn("dummyCol", row_number().over(vtr_windowspec)).drop("dummyCol").where('dummyCol <= 5)
    saveAsCSV(vtr_df,"C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/output/vtr1")

    //CMO want to track the campaign performance on below metrics for each campaign, partner and device:
    //1.CTR (Click Through Rate) [Hint: CTR = Clicks / Impressions]
    //2.CPC (Cost Per Click) [Hint: CPC = Total Cost of Clicks / Total Clicks]  //assuming totalcostofclicks=actualized_spend

    var cmo_df = media_DF.where('impressions.isNotNull || length(trim('impressions)) != "0").withColumn("ctr", $"clicks" / $"impressions").withColumn("cpc", $"Actualized_spend" / $"clicks").select($"partner", $"campaign", $"device", $"ctr", $"cpc", month(to_date($"date", "dd-MM-yyyy")).as("campaign_mnth"))

    var best_mnth = cmo_df.groupBy('campaign, 'device, 'partner, 'campaign_mnth).agg(max('ctr).as("ctr"), max('cpc).as("cpc")).select('campaign, 'device, 'partner, 'campaign_mnth.as("best_month"))

     saveAsCSV(best_mnth,"C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/output/cmo_mnth")

    //Find out what % of people has started watching the video but did not completed it for a given campaign and partner?
    var non_complete = media_DF.withColumn("non_complete", ($"Video_Views" - $"Video_Completes") / 100).select('campaign, 'partner, 'non_complete)

    media_DF.groupBy('campaign, 'partner, 'Video_Views, 'Video_Completes).agg((($"Video_Views" - $"Video_Completes") / 100).as("non_complete")).show(false)
    
    //Calculate the total number of visits for unique keywords for each publisher for both branded and nonbranded searches
    var vists_df = paid_DF.groupBy('publisher, 'Original_Keyword, 'Brand_Non_Brand).agg(count('Brand_Non_Brand).as("total_visits"))
    saveAsCSV(vists_df, "C:/Users/Mounisra1/OneDrive/Desktop/LTI-RelatedDOC/Citi_practise_Doc/output/visits")

    //Compare ER (Engagement Rate) for Video channels v/s Non-Video Channels? [Engagement R ate = Engagement / Impressions],engagements is null elimnateing
    //For channel=video -> eng is blank,channel =0 eng value present,channel=null -> eng=null

    var video_df = media_DF.where($"channel" === "video" && ($"engagements".isNotNull || length(trim($"engagements")) != 0 || $"engagements" != 0)) //.withColumn("eng_rate_video",$"engagements"/$"impressions").select('partner,'campaign,'eng_rate_video)

    //channel 0 records:28973; assuming blank for non video,cmpre partner,campaign
    var nonvideo_df = media_DF.where(($"channel".isNull || length(trim($"channel")) === 0) && ($"engagements".isNotNull || length(trim($"engagements")) != 0 || $"engagements" != 0)).withColumn("eng_rate_nonvideo", $"engagements" / $"impressions") select ('partner, 'campaign, 'eng_rate_nonvideo)

    var er_df = video_df.join(nonvideo_df, Seq("partner", "campaign"))

  }
}