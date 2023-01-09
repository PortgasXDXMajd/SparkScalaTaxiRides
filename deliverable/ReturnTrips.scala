import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row


object ReturnTrips {
  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {
    import spark.implicits._

    var timeDiff = 8 * 60 * 60
    var distanceBucketSize: Double = dist / 111100
    
    val makeDistExpr = (lat1 : Column, lon1 : Column, lat2 : Column, lon2 : Column) => {
      val earthRadius = 6371000
      val dLat = toRadians(abs(lat2 - lat1))
      val dLon = toRadians(abs(lon2 - lon1))
      val hav = pow(sin(dLat*0.5),2) + pow(sin(dLon*0.5),2) * cos(toRadians(lat1)) * cos(toRadians(lat2))
      abs(lit(earthRadius * 2) * asin(sqrt(hav)))
    }

    var customDf = trips.withColumn("pickupTime", unix_timestamp($"tpep_pickup_datetime"))
    customDf = customDf.withColumn("dropoffTime",  unix_timestamp($"tpep_dropoff_datetime"))

    customDf = customDf.select(
      $"pickupTime",
      $"dropoffTime",
      $"dropoff_latitude",
      $"dropoff_longitude",
      $"pickup_latitude",
      $"pickup_longitude")
      .cache()

    var explodedDf = customDf.withColumn("dropoffLatBucket", explode(array(floor($"dropoff_latitude" / distanceBucketSize) - 1,floor($"dropoff_latitude" / distanceBucketSize),floor($"dropoff_latitude" / distanceBucketSize) + 1)))
    explodedDf = explodedDf.withColumn("pickupLatBucket", explode(array(floor($"pickup_latitude" / distanceBucketSize) - 1,floor($"pickup_latitude" / distanceBucketSize),floor($"pickup_latitude" / distanceBucketSize) + 1)))
    explodedDf = explodedDf.withColumn("dropoffTimeBucket", explode(array(floor($"dropoffTime" / timeDiff),floor($"dropoffTime" / timeDiff) + 1 )))

    var bucketDf = customDf.withColumn("dropoffLatBucket", floor($"dropoff_latitude" / distanceBucketSize))
    bucketDf = bucketDf.withColumn("pickupLatBucket", floor($"pickup_latitude" / distanceBucketSize))
    bucketDf = bucketDf.withColumn("pickupTimeBucket", floor($"pickupTime" / timeDiff))

    var finalAns = explodedDf.as("a").join(bucketDf.as("b"),
      ($"a.dropoffLatBucket" === $"b.pickupLatBucket") &&
      ($"a.pickupLatBucket" === $"b.dropoffLatBucket") &&
      ($"a.dropoffTimeBucket" === $"b.pickupTimeBucket"))

    finalAns = finalAns.filter(
      ($"a.dropoffTime" < $"b.pickupTime") &&
      ($"a.dropoffTime" + timeDiff > $"b.pickupTime") &&
      (makeDistExpr($"a.dropoff_latitude",$"a.dropoff_longitude", $"b.pickup_latitude",$"b.pickup_longitude") < dist) &&
      (makeDistExpr($"a.pickup_latitude",$"a.pickup_longitude", $"b.dropoff_latitude",$"b.dropoff_longitude") < dist)
    )

    finalAns
  }
}