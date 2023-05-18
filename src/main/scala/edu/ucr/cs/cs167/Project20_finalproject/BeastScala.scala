package edu.ucr.cs.cs167.Project20_finalproject

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Wildfire Analysis
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Wildfire Analysis")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()

      operation match {
        case "data-prep" =>
          val outputFile: String = args(2)
          //Parse and load the CSV file using the Dataframe API
          val wildfireDF = sparkSession.read.format("csv")
            .option("sep", "\t")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(inputFile)

          //Reduce size of the dataset by keeping only the following columns: x, y, acq_date, frp, acq_time and fix up the 'frp' column to all be doubles
          val cleanedWildfireDF = wildfireDF.selectExpr("x", "y", "acq_date", "double(split(frp, ',')[0]) AS frp", "acq_time")

          //Convert the resulting Dataframe to a SpatialRDD including the geometry attribute
          val wildfireRDD = cleanedWildfireDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry").toSpatialRDD

          //Load County dataset using Beast
          val countiesRDD = sparkContext.shapefile("tl_2018_us_county.zip")

          //Run a spatial join query to find the county of each wildfire
          val wildfireCountyRDD: RDD[(IFeature, IFeature)] = wildfireRDD.spatialJoin(countiesRDD)

          //Use GEOID attribute to introduce a new attribute County in the wildfire and convert it back to a dataframe
          val wildfireCounty = wildfireCountyRDD.map({ case (wildfire, county) => Feature.append(wildfire, county.getAs[String]("GEOID"), "County") })
            .toDataFrame(sparkSession)

          //Drop the geometry column
          val wildfireCountyDF = wildfireCounty.drop("geometry")

          //write the output as a Parquet file
          wildfireCountyDF.write.mode(SaveMode.Overwrite).parquet(outputFile)

        case "spatial-analysis" =>
          val outputFile: String = args(2)
          val start: String = args(3)
          val end: String = args(4)

          //Load Parquet file
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("wildfires")

          // SQL to filter start and end date
          sparkSession.sql(
            sqlText =
              s"""
                 SELECT County, sum(frp) AS fire_intensity
                 FROM wildfires
                 WHERE acq_date between to_date("$start","MM/dd/yyyy") and to_date("$end","MM/dd/yyyy")
                 GROUP BY County
              """).createOrReplaceTempView("fire")

          //Load County file
          sparkContext.shapefile("tl_2018_us_county.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("counties")

          //Join county and wildfire by GEOID = County
          sparkSession.sql(
            s"""
                      SELECT GEOID, NAME, g, fire_intensity
                      FROM fire, counties
                      WHERE GEOID = County
                    """).toSpatialRDD
            .coalesce(1)
            .saveAsShapefile(outputFile)

        case "temporal-analysis" =>
          //the temporal_output folder contains the csv file
          val outputFile: String = args(2)
          val countyName: String = args(3)
          //read in the parquet file
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("wildfires")

          val wildfireDF = sparkSession.read.parquet(inputFile)

          //load county dataset
          sparkContext.shapefile("tl_2018_us_county.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("counties")

          //filter counties by CA STATEFP and user county name to find county GEOID
          val countiesDF: DataFrame = sparkSession.sql(
            sqlText =
              s"""
                 SELECT GEOID, Name
                 FROM counties
                 WHERE STATEFP="06" AND Name = "$countyName"
              """)

          //store value of the county's GEOID
          val countyGEOID = countiesDF.head().getString(0)

          //select wildfires related to county, compute SUM(frp). sort by date
          val intensityDF: DataFrame = sparkSession.sql(
            s"""
                 SELECT DATE_FORMAT(acq_date, "yyyy-MM") AS year_month, SUM(frp) AS fire_intensity
                 FROM wildfires
                 WHERE County = "$countyGEOID"
                 GROUP BY acq_date
                 ORDER BY acq_date
              """)

          //write output to csv file
          intensityDF.write.mode(SaveMode.Overwrite).csv(outputFile)

      }
      val t2 = System.nanoTime()
      println(s"Command '$operation' on file '$inputFile' took ${(t2-t1)*1E-9} seconds")
    } finally {
      sparkSession.stop()
    }
  }
}