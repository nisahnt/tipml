package com.ml.gyc

import com.ml.gyc.helper.Utils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession

object TaxiTipTrainer {

  def main(args: Array[String]) {

    val sparkConfig = new SparkConf().setMaster(args(0)).setAppName("Taxi Tip Amount Trainer")

    val sparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
    val tripDf = sparkSession.read.option("header", true).option("inferSchema", true).csv(args(1)).filter("lpep_pickup_datetime between '01/01/2017%' and '01/06/2017%'")

    println("tripDf.count: "+tripDf.count())

    // drop
    val df1 = tripDf.drop("VendorID").drop("store_and_fwd_flag").drop("lpep_pickup_datetime").drop("lpep_dropoff_datetime").drop("PULocationID").drop("trip_type")
      .drop("ehail_fee").drop("RatecodeID").drop("tolls_amount").drop("mta_tax").drop("DOLocationID").drop("passenger_count")
        .drop("extra").drop("trip_type").drop("fare_amount").drop("total_amount").drop("improvement_surcharge")

    val df2 = df1.withColumn("trip_distance", df1("trip_distance").cast("double"))
      .withColumn("tip_amount", df1("tip_amount").cast("double")).withColumn("payment_type", df1("payment_type").cast("int"))

    // drop label and create feature vector
    df2.cache()
    val df3 = df2.drop("tip_amount")
    val featureCols = df3.columns

    val vectorAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("rawFeatures")

    val vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features")

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("tip_amount")

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, vectorIndexer, lr))

    //val Array(training, test) = df2.randomSplit(Array(0.8, 0.2), seed = 12345)

    val model = pipeline.fit(df2)

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    Utils.deletePath(fs,args(2))
    sparkSession.sparkContext.parallelize(Seq(model),1).saveAsObjectFile(args(2))

/*  val fullPredictions = model.transform(test).cache()

    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select("tip_amount").rdd.map(_.getDouble(0))
    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
    println(s"  Root mean squared error (RMSE): $RMSE")*/

    if (sparkSession != null) {
      sparkSession.stop()
    }
  }
}
