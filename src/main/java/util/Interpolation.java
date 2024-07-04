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

    public static Dataset<Row> sqlVersionOfInverseDistanceWeightingAlgorithm(SparkSession spark, Dataset<Row> knownLocations, Dataset<Row> unknownLocations) {
        knownLocations.createOrReplaceTempView("knownLocations");
        unknownLocations.createOrReplaceTempView("unknownLocations");

        Dataset<Row> crossJoined = spark.sql("SELECT unknownLocations.*, knownLocations.* FROM unknownLocations CROSS JOIN knownLocations");
        crossJoined.createOrReplaceTempView("crossJoined");

        spark.udf().register("calculateDistance", (Double lat1, Double lon1, Double lat2, Double lon2) -> getDistanceBetweenTwoPoints(lat1, lon1, lat2, lon2), DataTypes.DoubleType);

        Dataset<Row> withDistances = spark.sql("SELECT crossJoined.*, calculateDistance(unknownLatitude, unknownLongitude, knownLatitude, knownLongitude) AS distance FROM crossJoined");
        crossJoined.unpersist();
        withDistances.createOrReplaceTempView("withDistances");

        Dataset<Row> withDistancePoweredToPowerParameter = spark.sql("SELECT withDistances.*, POW(withDistances.distance, -3) AS weight FROM withDistances");
        withDistances.unpersist();
        withDistancePoweredToPowerParameter.createOrReplaceTempView("withDistancePoweredToPowerParameter");

        Dataset<Row> withWeightedTemperatures = spark.sql("SELECT withDistancePoweredToPowerParameter.*, withDistancePoweredToPowerParameter.knownTemperature * withDistancePoweredToPowerParameter.weight AS weightedTemperature FROM withDistancePoweredToPowerParameter");
        withDistancePoweredToPowerParameter.unpersist();
        withWeightedTemperatures.createOrReplaceTempView("withWeightedTemperatures");

        Dataset<Row> weightedSum = spark.sql("SELECT unknownLatitude, unknownLongitude, SUM(weightedTemperature) AS sumWeightedTemperature, SUM(weight) AS sumWeight FROM withWeightedTemperatures GROUP BY unknownLatitude, unknownLongitude");
        withWeightedTemperatures.unpersist();
        weightedSum.createOrReplaceTempView("weightedSum");

        Dataset<Row> predictedLocations = spark.sql("SELECT weightedSum.*, weightedSum.sumWeightedTemperature / weightedSum.sumWeight AS predictedTemperature FROM weightedSum");
        weightedSum.unpersist();
        predictedLocations.createOrReplaceTempView("predictedLocations");

        return spark.sql("SELECT predictedLocations.unknownLatitude AS latitude, predictedLocations.unknownLongitude AS longitude, ROUND(predictedLocations.predictedTemperature, 1) AS temperature FROM predictedLocations");
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
