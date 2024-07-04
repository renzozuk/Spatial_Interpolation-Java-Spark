package util;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static util.Math.DEGREES_TO_RADIANS;
import static util.Math.pow;

public class Interpolation {
    public static JavaRDD<double[]> inverseDistanceWeightingAlgorithm(JavaRDD<double[]> knownLocations, JavaRDD<double[]> unknownLocations) {
        JavaPairRDD<double[], double[]> crossJoined = unknownLocations.cartesian(knownLocations);

        JavaPairRDD<Tuple2<Double, Double>, Tuple2<Double, Double>> distancesAndWeights = crossJoined.mapToPair((PairFunction<Tuple2<double[], double[]>, Tuple2<Double, Double>, Tuple2<Double, Double>>) tuple -> {
            double weight = pow(getDistanceBetweenTwoPoints(tuple._1[0], tuple._1[1], tuple._2[0], tuple._2[1]), -3);

            return new Tuple2<>(new Tuple2<>(tuple._1[0], tuple._1[1]), new Tuple2<>(tuple._2[2] * weight, weight));
        });

        JavaPairRDD<Tuple2<Double, Double>, Tuple2<Double, Double>> summedWeightsAndTemperatures = distancesAndWeights.reduceByKey((Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>) (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        return summedWeightsAndTemperatures.map(tuple -> new double[]{tuple._1._1, tuple._1._2, tuple._2._1 / tuple._2._2});
    }

    private static double getDistanceBetweenTwoPoints(double lat1, double lon1, double lat2, double lon2) {
        double dLat = (lat2 - lat1) * DEGREES_TO_RADIANS;
        double dLon = (lon2 - lon1) * DEGREES_TO_RADIANS;

        double a = sin(dLat / 2.0) * sin(dLat / 2.0) +
                cos(lat1 * DEGREES_TO_RADIANS) * cos(lat2 * DEGREES_TO_RADIANS) *
                sin(dLon / 2.0) * sin(dLon / 2.0);

        return 12742.0 * atan2(sqrt(a), sqrt(1 - a));
    }
}
