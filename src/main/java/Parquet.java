import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static util.Interpolation.inverseDistanceWeightingAlgorithm;

public class Parquet {
    public static void main(String[] args) {
        final long checkpoint1 = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Spatial-Interpolation")
                .config("spark.driver.maxResultSize", "2g")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> knownPoints = spark.read().format("parquet")
                .option("header", "false")
                .load("src/main/resources/known_locations.parquet")
                .withColumnRenamed("coluna_1", "knownLatitude")
                .withColumnRenamed("coluna_2", "knownLongitude")
                .withColumnRenamed("coluna_3", "knownTemperature");

        Dataset<Row> unknownPoints = spark.read().format("parquet")
                .option("header", "false")
                .load("src/main/resources/unknown_locations.parquet")
                .withColumnRenamed("coluna_1", "unknownLatitude")
                .withColumnRenamed("coluna_2", "unknownLongitude");

        Dataset<Row> output = inverseDistanceWeightingAlgorithm(spark, knownPoints, unknownPoints);
        knownPoints.unpersist();
        unknownPoints.unpersist();

        output.write().format("parquet")
                .option("header", "false")
                .save("src/main/resources/output/parquet");

        spark.stop();

        final long checkpoint2 = System.currentTimeMillis();

        System.out.printf("Total time: %.3fs%n", (checkpoint2 - checkpoint1) / 1e3);
    }
}
