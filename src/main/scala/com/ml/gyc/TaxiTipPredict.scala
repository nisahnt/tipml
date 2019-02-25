package com.ml.gyc

import com.ml.gyc.helper.Utils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}

object TaxiTipPredict {

  def main(args: Array[String]) {
    val sparkConfig = new SparkConf().setMaster(args(0)).setAppName("Taxi Tip Amount Predict")

    val sparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

    val tripDf = sparkSession.read.option("header", true).option("inferSchema", true).csv(args(1)).filter("lpep_pickup_datetime between '01/07/2017%' and '01/13/2017%'")

    // drop
    val df1 = tripDf.drop("VendorID").drop("store_and_fwd_flag").drop("lpep_pickup_datetime").drop("lpep_dropoff_datetime").drop("PULocationID").drop("trip_type")
      .drop("ehail_fee").drop("RatecodeID").drop("tolls_amount").drop("mta_tax").drop("DOLocationID").drop("passenger_count")
      .drop("extra").drop("trip_type").drop("fare_amount").drop("total_amount").drop("improvement_surcharge").drop("tip_amount")

    val df = df1.withColumn("trip_distance", df1("trip_distance").cast("double"))
      .withColumn("payment_type", df1("payment_type").cast(IntegerType))
    df.cache()

    val model = sparkSession.sparkContext.objectFile[PipelineModel](args(2)).first()
    import sparkSession.implicits._
    val fullPredictions = model.transform(df).drop("rawFeatures").drop("features").rdd.map(r => {
      val prediction = r.getAs[Double]("prediction")
      (r.getAs[Double]("trip_distance"), r.getAs[Int]("payment_type"), if (prediction < 0.0) 0 else prediction)
    }).toDF("trip_distance", "payment_type", "predicted_tip")

    fullPredictions.show()
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

    Utils.deletePath(fs,args(3))
    fullPredictions.write.option("header", true).mode(SaveMode.Overwrite).csv(args(3))

    if (sparkSession != null) {
      sparkSession.stop()
    }

  }
}
