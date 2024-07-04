import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static util.Interpolation.inverseDistanceWeightingAlgorithm;
import static util.Schema.knownPointsSchema;
import static util.Schema.unknownPointsSchema;

public class CSV {
    public static void main(String[] args) {
        final long checkpoint1 = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Test")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> knownPoints = spark.read().format("csv")
                .option("header", "false")
                .option("sep", ";")
                .schema(knownPointsSchema)
                .load("src/main/resources/known_locations.csv");

        Dataset<Row> unknownPoints = spark.read().format("csv")
                .option("header", "false")
                .option("sep", ";")
                .schema(unknownPointsSchema)
                .load("src/main/resources/unknown_locations.csv");

        Dataset<Row> output = inverseDistanceWeightingAlgorithm(spark, knownPoints, unknownPoints);
        knownPoints.unpersist();
        unknownPoints.unpersist();

        output.write().format("csv")
                .option("header", "false")
                .option("sep", ";")
                .save("src/main/resources/output/csv");

        spark.stop();

        final long checkpoint2 = System.currentTimeMillis();

        System.out.printf("Total time: %.3fs%n", (checkpoint2 - checkpoint1) / 1e3);
    }
}
