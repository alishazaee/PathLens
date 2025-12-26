package org.shazaei;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.classic.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.shazaei.configs.ApplicationConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import org.shazaei.extract.TaxiLocationExtract;
import org.shazaei.loader.TaxiLocationLoader;
import org.shazaei.protobuf.TaxiLoc;
import org.shazaei.transform.TaxiLocationTransform;
import org.yaml.snakeyaml.Yaml;

public class App
{

    public static void Start(ApplicationConfig config) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("TaxiLocationStreamingApp")
                .getOrCreate();
        TaxiLocationExtract extractor = new TaxiLocationExtract(spark,config.getExtractorConfig());
        TaxiLocationLoader loader = new TaxiLocationLoader(config.getLoaderConfig());
        TaxiLocationTransform transformer = new TaxiLocationTransform(config.getTransformConfig());

        Dataset<TaxiLoc.TaxiLocation> extractedDf = extractor.extract();
        Dataset<Row> transformedDf = transformer.transform(extractedDf);
        StreamingQuery query = loader.load(transformedDf);
        query.awaitTermination();
    }


    public static void main(String[] args)
    {
        ApplicationConfig config = loadConfig();
        try {
            Start(config);
        }
        catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        }

    }

    public static ApplicationConfig loadConfig() {
        try {
            ObjectMapper mapper = new YAMLMapper();
            InputStream is = App.class.getClassLoader()
                    .getResourceAsStream("application.yml");
            return mapper.readValue(is, ApplicationConfig.class);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load test configuration", e);
        }
    }
}
