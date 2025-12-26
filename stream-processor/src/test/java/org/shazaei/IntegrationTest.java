package org.shazaei;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.classic.StreamingQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.shazaei.configs.ApplicationConfig;
import org.shazaei.extract.TaxiLocationExtract;
import org.shazaei.loader.TaxiLocationLoader;
import org.shazaei.protobuf.TaxiLoc;
import org.shazaei.transform.TaxiLocationTransform;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static org.shazaei.Util.createLocation;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class IntegrationTest {
    private KafkaContainer kafkaContainer;
    private KafkaProducer<byte[], byte[]> producer;
    private KafkaConsumer<byte[], byte[]> consumer;
    List<List<Object>> sampleData;
    SparkSession spark;
    ApplicationConfig applicationConfig;

    private SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("test")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName() );
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    @BeforeEach
    void setUp() throws IOException {
        kafkaContainer = new KafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
        );
        kafkaContainer.start();
        spark = createSparkSession();
        applicationConfig = App.loadConfig();
        sampleData = List.of(
                List.of("taxi-1", "trip-1", 40.7128, -74.0060, 1000L),
                List.of("taxi-1", "trip-1", 40.7131, -74.0061, 1001L),
                List.of("taxi-1", "trip-1", 40.7132, -74.0061, 1001L),
                List.of("taxi-1", "trip-1", 40.7130, -74.0062, 1005L),
                List.of("taxi-1", "trip-1", 60.7132, 10.0064, 1007L),
                List.of("taxi-2", "trip-2", 60.7132, 10.0064, 1007L),
                List.of("taxi-2", "trip-2", 60.7135, 10.0062, 1007L)
        );
        producer = createProducer();
        consumer = createConsumer();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        applicationConfig.getExtractorConfig().getOptions()
                .put("kafka.bootstrap.servers", bootstrapServers);
        applicationConfig.getLoaderConfig().getOptions()
                .put("kafka.bootstrap.servers", bootstrapServers);
        Path checkpoint = Paths.get(applicationConfig.getLoaderConfig().getOptions().get("checkpointLocation"));
        if (Files.exists(checkpoint)) {
            FileUtils.deleteDirectory(checkpoint.toFile());
        }

    }


    public void ingestLogs() throws ExecutionException, InterruptedException {
        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (List<Object> data : sampleData) {
            records.add(new ProducerRecord<>(applicationConfig.getExtractorConfig().getOptions().get("subscribe"), createLocation(
                    (String) data.get(0),
                    (String) data.get(1),
                    (Double) data.get(2),
                    (Double) data.get(3),
                    (Long) data.get(4)).toByteArray()));
        }
        for (ProducerRecord<byte[], byte[]> record : records) {
            producer.send(record).get();
        }
    }

    @Test
    public void testKafkaToKafkaIntegration() throws Exception {
        TaxiLocationExtract extractor = new TaxiLocationExtract(spark, applicationConfig.getExtractorConfig());
        TaxiLocationTransform transformer = new TaxiLocationTransform(applicationConfig.getTransformConfig());
        TaxiLocationLoader loader = new TaxiLocationLoader(applicationConfig.getLoaderConfig());

        Dataset<TaxiLoc.TaxiLocation> extractedDf = extractor.extract();
        Dataset<Row> transformedDf = transformer.transform(extractedDf);
        StreamingQuery query = loader.load(transformedDf);

        String outputTopic = applicationConfig.getLoaderConfig().getOptions().get("topic");
        consumer.subscribe(Collections.singletonList(outputTopic));

        ingestLogs();

        query.processAllAvailable();
        await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> consumer.poll(Duration.ofMillis(1000)).count() == 3);

        query.stop();
    }


    @AfterEach
    public void tearDown(){
        kafkaContainer.stop();
        if ( spark != null ) {
            spark.stop();
        }
    }

}
