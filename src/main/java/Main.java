import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

import static java.lang.String.format;
import static util.Interpolation.inverseDistanceWeightingAlgorithm;

public class Main {
    public static void main(String[] args) {
        final long checkpoint1 = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf().set("spark.driver.maxResultSize", "2g").setAppName("Spatial Interpolation").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> linesFromKnownPoints = sc.textFile("src/main/resources/known_locations.csv");

            JavaRDD<double[]> knownPoints = linesFromKnownPoints.map(l -> l.split(";"))
                    .map(a -> Arrays.stream(a).mapToDouble(Double::parseDouble).toArray());

            linesFromKnownPoints.unpersist();

            JavaRDD<String> linesFromUnknownPoints = sc.textFile("src/main/resources/unknown_locations.csv");

            JavaRDD<double[]> unknownPoints = linesFromUnknownPoints.map(l -> l.split(";"))
                    .map(a -> Arrays.stream(a).mapToDouble(Double::parseDouble).toArray());

            linesFromUnknownPoints.unpersist();

            JavaRDD<double[]> interpolatedUnknownPoints = inverseDistanceWeightingAlgorithm(knownPoints, unknownPoints);

            knownPoints.unpersist();
            unknownPoints.unpersist();

            JavaRDD<String> output = interpolatedUnknownPoints.map(a -> format("%.6f;%.6f;%.1f", a[0], a[1], a[2]));

            interpolatedUnknownPoints.unpersist();

//            for(String line : output.collect()){
//                System.out.println(line);
//            }

            output.saveAsTextFile("src/main/resources/output");
        }

        final long checkpoint2 = System.currentTimeMillis();

        System.out.printf("Total time: %.3fs%n", (checkpoint2 - checkpoint1) / 1e3);
    }
}
