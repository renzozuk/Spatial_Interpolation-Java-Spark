package util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static util.Math.DEGREES_TO_RADIANS;

public class Interpolation {
    public static Dataset<Row> inverseDistanceWeightingAlgorithm(SparkSession spark, Dataset<Row> knownLocations, Dataset<Row> unknownLocations) {
        Dataset<Row> crossJoined = unknownLocations.crossJoin(knownLocations);

        spark.udf().register("calculateDistance", (Double lat1, Double lon1, Double lat2, Double lon2) -> getDistanceBetweenTwoPoints(lat1, lon1, lat2, lon2), DataTypes.DoubleType);

        Dataset<Row> withDistances = crossJoined.withColumn("distance", callUDF("calculateDistance",
                crossJoined.col("unknownLatitude"),
                crossJoined.col("unknownLongitude"),
                crossJoined.col("knownLatitude"),
                crossJoined.col("knownLongitude")));

        crossJoined.unpersist();

        Dataset<Row> withDistancePoweredToPowerParameter = withDistances.withColumn("weight", pow(col("distance"), -3));

        withDistances.unpersist();

        Dataset<Row> withWeightedTemperatures = withDistancePoweredToPowerParameter.withColumn("weightedTemperature", col("knownTemperature").multiply(col("weight")));

        withDistancePoweredToPowerParameter.unpersist();

        Dataset<Row> weightedSum = withWeightedTemperatures.groupBy("unknownLatitude", "unknownLongitude")
                .agg(sum("weightedTemperature").alias("sumWeightedTemperature"),
                sum("weight").alias("sumWeight"));

        withWeightedTemperatures.unpersist();

        Dataset<Row> predictedLocations = weightedSum.withColumn("predictedTemperature", round(col("sumWeightedTemperature").divide(col("sumWeight")), 1));

        weightedSum.unpersist();

        return predictedLocations.select("unknownLatitude", "unknownLongitude", "predictedTemperature")
                .withColumnRenamed("unknownLatitude", "latitude")
                .withColumnRenamed("unknownLongitude", "longitude")
                .withColumnRenamed("predictedTemperature", "temperature");
    }

    public static double getDistanceBetweenTwoPoints(double lat1, double lon1, double lat2, double lon2) {
        double dLat = (lat2 - lat1) * DEGREES_TO_RADIANS;
        double dLon = (lon2 - lon1) * DEGREES_TO_RADIANS;

        double a = sin(dLat / 2.0) * sin(dLat / 2.0) +
                cos(lat1 * DEGREES_TO_RADIANS) * cos(lat2 * DEGREES_TO_RADIANS) *
                sin(dLon / 2.0) * sin(dLon / 2.0);

        return 12742.0 * atan2(sqrt(a), sqrt(1 - a));
    }
}
