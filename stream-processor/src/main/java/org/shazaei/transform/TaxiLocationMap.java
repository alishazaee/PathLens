package org.shazaei.transform;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.shazaei.protobuf.TaxiLoc;

public class TaxiLocationMap implements MapFunction<TaxiLoc.TaxiLocation, Row> {

    public static final String TAXI_ID_COLUMN = "taxi_id";
    public static final String LATITUDE_COLUMN = "lat";
    public static final String LONGITUDE_COLUMN = "long";
    public static final String TIMESTAMP_COLUMN = "timestamp";
    public static final String GEOHASH_COLUMN = "geohash";
    public static final String TRIP_ID_COLUMN = "trip_id";
    public final int Precision;

    public static final StructType SCHEMA = new StructType()
            .add(TAXI_ID_COLUMN, DataTypes.StringType)
            .add(TIMESTAMP_COLUMN, DataTypes.LongType)
            .add(TRIP_ID_COLUMN, DataTypes.StringType)
            .add(LATITUDE_COLUMN, DataTypes.DoubleType)
            .add(LONGITUDE_COLUMN, DataTypes.DoubleType)
            .add(GEOHASH_COLUMN, DataTypes.StringType);

    public TaxiLocationMap(int Precision) {
        super();
        this.Precision = Precision;
    }

    @Override
    public Row call(TaxiLoc.TaxiLocation log) throws Exception {
        if (log == null || !log.hasLatitude() || !log.hasLongitude()) {
            return Row.empty();
        }
        String geohash = GeoHash.withCharacterPrecision(
                log.getLatitude(), log.getLongitude(), Precision).toBase32();
        return RowFactory.create(
                log.getTaxiId(),
                log.getTimestamp(),
                log.getTripId(),
                log.getLatitude(),
                log.getLongitude(),
                geohash
        );
    }
}
