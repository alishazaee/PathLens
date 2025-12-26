package org.shazaei.extract;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.shazaei.ByteArrayToProtoLogMapper;
import org.shazaei.configs.ExtractorConfig;
import org.shazaei.protobuf.TaxiLoc;

public class TaxiLocationExtract {
    private final SparkSession sparkSession;
    private final ExtractorConfig extractorConfig;

    public TaxiLocationExtract(SparkSession sparkSession, ExtractorConfig extractorConfigs) {
        this.sparkSession = sparkSession;
        this.extractorConfig = extractorConfigs;
    }

    public Dataset<TaxiLoc.TaxiLocation> extract() {
        return sparkSession
                .readStream()
                .format(extractorConfig.getFormat())
                .options(extractorConfig.getOptions())
                .load()
                .selectExpr("CAST(value AS BINARY) AS protoData")
                .as(Encoders.BINARY())
                .map((MapFunction<byte[], TaxiLoc.TaxiLocation>) ByteArrayToProtoLogMapper::toProtoLog, Encoders.kryo(TaxiLoc.TaxiLocation.class));
    }
}
