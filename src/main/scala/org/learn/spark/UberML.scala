package org.learn.spark

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

/**
 * Hello world!
 *
 */
object UberML extends App {

  // Set up project
  val conf = new SparkConf().setAppName("UberModel").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  // Define structure
  val schema = StructType(Array(
    StructField("dt", TimestampType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true)
  ))

  // Read the csv
  val csvFile = sc.textFile("/user/cloudera/uber")
  val rows = csvFile.map(line => line.replaceAll("\"", "").split(","))
  // Get the header and filter from the data
  val header = rows.first
  val data = rows.filter(_(0) != header(0))
  // Get the data like a rdd
  val NEW_FORMAT = "dd/MM/yyyy HH:mm:ss"
  val dateFormat = new SimpleDateFormat(NEW_FORMAT)
  val dataRDD = data.map(row => Row(new Timestamp(dateFormat.parse(row(0)).getTime), row(1).toDouble, row(2).toDouble, row(3)))

  // Convert to Dataframe
  val uberDF = sqlContext.createDataFrame(dataRDD, schema)

  // Create new column with lat, long
  val featureCols = Array("lat", "lon")
  val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
  val uberTransfDF = assembler.transform(uberDF)

  // Settings for the kmeans
  val Array(trainingData, testData) = uberTransfDF.randomSplit(Array(0.7, 0.3), 5043)
  val kmeans = new KMeans().setK(8).setFeaturesCol("features").setPredictionCol("prediction")
  val model = kmeans.fit(uberTransfDF)
  println("Final centers: ")
  model.clusterCenters.foreach(println)

  val categories = model.transform(testData)

  // Which hours of the day and which cluster had the highest number of pickups
  categories.select(hour($"dt").alias("hour"), $"prediction").groupBy("hour", "prediction")
    .agg(count("prediction").alias("count")).orderBy(desc("count")).show

  // how many pickups occurred in each cluster
  categories.groupBy("prediction").count().show

  categories.registerTempTable("uber")
  sqlContext.sql("SELECT prediction, COUNT(prediction) AS count FROM uber GROUP BY prediction").show

  sqlContext.sql("SELECT hour(uber.dt) AS hr, COUNT(prediction) AS ct FROM uber GROUP BY hour(uber.dt)").show

  // To save the model
  model.write.overwrite.save("/user/cloudera/uber/model")
  val res = sqlContext.sql("select dt,lat,lon,base,prediction as cid from uber order by dt")

  res.write.format("json").save("/user/cloudera/uber/output/uber.json")
}
