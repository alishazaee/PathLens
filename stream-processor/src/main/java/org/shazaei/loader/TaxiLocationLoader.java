package org.shazaei.loader;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.classic.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.shazaei.configs.LoaderConfig;
import org.shazaei.protobuf.TaxiLoc;

import java.util.concurrent.TimeoutException;

import static org.shazaei.transform.TaxiLocationMap.*;

public class TaxiLocationLoader {
    private final LoaderConfig loaderConfig;

    private static final StructType OUTPUT_SCHEMA = new StructType()
            .add("key", DataTypes.BinaryType)
            .add("value", DataTypes.BinaryType);

    public TaxiLocationLoader(LoaderConfig config) {
        loaderConfig = config;
    }

    public StreamingQuery load(Dataset<Row> resultDf) throws TimeoutException {
        return (StreamingQuery) resultDf
                .map((MapFunction<Row, Row>) TaxiLocationLoader::getLocationKeyValue,
                        Encoders.row(OUTPUT_SCHEMA))
                .writeStream()
                .options(loaderConfig.getOptions())
                .format(loaderConfig.getFormat())
                .outputMode(loaderConfig.getOutputMode())
                .start();
    }

    private static Row getLocationKeyValue(Row row) {
         TaxiLoc.TaxiLocation location = TaxiLoc.TaxiLocation.newBuilder()
                .setTaxiId(row.getAs(TAXI_ID_COLUMN))
                .setTripId(row.getAs(TRIP_ID_COLUMN))
                .setLatitude(row.getAs(LATITUDE_COLUMN))
                .setLongitude(row.getAs(LONGITUDE_COLUMN))
                .setTimestamp(row.getAs(TIMESTAMP_COLUMN))
                .build();
         byte[] locationBytes = location.toByteArray();
         return RowFactory.create(
                 row.getAs(TAXI_ID_COLUMN).toString().getBytes(),
                 locationBytes
         );
    }

}
