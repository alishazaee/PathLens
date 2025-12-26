package org.shazaei;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.shazaei.configs.ApplicationConfig;
import org.shazaei.protobuf.TaxiLoc;
import org.shazaei.transform.TaxiLocationMap;
import org.shazaei.transform.TaxiLocationTransform;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.shazaei.Util.createLocation;


public class TestTransformer {
    ApplicationConfig applicationConfig;
    SparkSession spark;

    @BeforeEach
    public void setup() {
        this.applicationConfig = App.loadConfig();
        spark = createSparkSession();
    }

    private SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("test")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    public void assertRowData(Row row,
                              String expectedTaxiId,
                              String expectedTripId,
                              List<Double> longitudes,
                              List<Double> latitudes){
        assertEquals(expectedTaxiId, row.getAs(TaxiLocationMap.TAXI_ID_COLUMN));
        assertEquals(expectedTripId,row.getAs(TaxiLocationMap.TRIP_ID_COLUMN));
        assertNotNull(row.getAs(TaxiLocationMap.LATITUDE_COLUMN));
        assertNotNull(row.getAs(TaxiLocationMap.LONGITUDE_COLUMN));
        assertEquals(row.getAs(TaxiLocationMap.LATITUDE_COLUMN),
                latitudes
                        .stream()
                        .mapToDouble(Double::doubleValue)
                        .average()
                        .orElseThrow());
        assertEquals(row.getAs(TaxiLocationMap.LONGITUDE_COLUMN),
                longitudes
                        .stream()
                        .mapToDouble(Double::doubleValue)
                        .average()
                        .orElseThrow());
    }

    @Test
    public void testCorrectAggregation(){
        Util util = new Util(spark);
        Dataset<TaxiLoc.TaxiLocation> inputDf = util.createTestDataset(
                createLocation("taxi-1", "trip-1", 40.7128, -74.0060, 1000),
                createLocation("taxi-1", "trip-1", 40.7130, -74.0062, 1005),
                createLocation("taxi-1", "trip-1", 60.7132, 10.0064, 1007),
                createLocation("taxi-2", "trip-2", 60.7132, 10.0064, 1007)
        );

        TaxiLocationTransform transform = new TaxiLocationTransform(applicationConfig.getTransformConfig());
        Dataset<Row> result = transform.transform(inputDf);
        List<Row> rows = result.collectAsList();

        assertEquals(3, rows.size());

        assertRowData(rows.get(0), "taxi-1", "trip-1", List.of(-74.0060,-74.0062), List.of(40.7128,40.7130));
        assertRowData(rows.get(1), "taxi-1", "trip-1",  List.of(10.0064), List.of(60.7132) );
        assertRowData(rows.get(2), "taxi-2", "trip-2", List.of(10.0064), List.of(60.7132) );
    }

    @AfterEach
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

}