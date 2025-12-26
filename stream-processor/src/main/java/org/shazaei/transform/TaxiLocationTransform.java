package org.shazaei.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.shazaei.configs.TransformConfig;
import org.shazaei.protobuf.TaxiLoc;
import static org.apache.spark.sql.functions.*;
import static org.shazaei.transform.TaxiLocationMap.*;

public class TaxiLocationTransform {
    private final static String PROCESSING_TIME_COLUMN= "processing_time";
    private final TransformConfig config;

    public TaxiLocationTransform(TransformConfig config) {
        this.config = config;
    }

    public Dataset<Row>  transform(Dataset<TaxiLoc.TaxiLocation> taxiLocationDataset) {
        return taxiLocationDataset
                .map(new TaxiLocationMap(config.getPrecision()),
                        RowEncoder.encoderFor(TaxiLocationMap.SCHEMA))
                .withColumn(PROCESSING_TIME_COLUMN, current_timestamp())
                .groupBy(
                        col(GEOHASH_COLUMN),
                        col(TAXI_ID_COLUMN),
                        col(TRIP_ID_COLUMN),
                        window(col(PROCESSING_TIME_COLUMN), config.getWindowSize()).alias("window")
                )
                .agg(
                        avg(LATITUDE_COLUMN).alias(LATITUDE_COLUMN),
                        avg(LONGITUDE_COLUMN).alias(LONGITUDE_COLUMN)
                )
                .withColumn(
                        TIMESTAMP_COLUMN,
                        expr(
                                "CAST((window.start + (window.end - window.start) / 2) AS BIGINT)"
                        )
                )
                .drop("window");
    }

}
