package org.shazaei;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.shazaei.protobuf.TaxiLoc;

import java.util.List;

public class Util {
    private final SparkSession spark;

    public Util(SparkSession sparkSession) {
        this.spark = sparkSession;
    }

    public Dataset<TaxiLoc.TaxiLocation> createTestDataset(TaxiLoc.TaxiLocation... locations){
        return spark.createDataset(
                List.of(locations),
                Encoders.javaSerialization(TaxiLoc.TaxiLocation.class)
        );
    }

    public static TaxiLoc.TaxiLocation createLocation(String taxiId,
                                                      String tripId,
                                                      double latitude, 
                                                      double longitude,
                                                      long timestamp) {
        return TaxiLoc.TaxiLocation.newBuilder()
                .setTaxiId(taxiId)
                .setTripId(tripId)
                .setLatitude(latitude)
                .setLongitude(longitude)
                .setTimestamp(timestamp)
                .build();
    }
}
