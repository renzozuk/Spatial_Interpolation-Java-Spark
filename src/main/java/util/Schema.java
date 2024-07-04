package util;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class Schema {
    public static final StructType knownPointsSchema = createStructType(new StructField[] {
            createStructField(
                    "knownLatitude",
                    DoubleType,
                    false
            ),
            createStructField(
                    "knownLongitude",
                    DoubleType,
                    false
            ),
            createStructField(
                    "knownTemperature",
                    DoubleType,
                    false
            )
    });

    public static final StructType unknownPointsSchema = createStructType(new StructField[] {
            createStructField(
                    "unknownLatitude",
                    DoubleType,
                    false
            ),
            createStructField(
                    "unknownLongitude",
                    DoubleType,
                    false
            )
    });
}
