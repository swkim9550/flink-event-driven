
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

//Test Code

@Slf4j
public class FlinkEventApplication implements Runnable {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    //kafka 
    private String bootstrapServers = "your kafka";
    
    List<String> topics = new ArrayList<>();

    //flinkGroupId
    private String flinkGroupId = "flink-test";


    public void run() {
        topics.add("flink.test");
        topics.add("flink.test2");

        KafkaSource<Tuple2<String,String>> source = KafkaSource.<Tuple2<String,String>>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(flinkGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new CustomKafkaDeserializationSchema()) // custom
                .build();

        DataStream<Tuple2<String,String>> inputStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        inputStreamSource.print();
        
        

        //ElasticSearch Sink
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("ip", 9200, "http"));
        httpHosts.add(new HttpHost("ip", 9200, "http"));

        ElasticsearchSink.Builder<Tuple2<String,String>> esSinkBuilder =
                new ElasticsearchSink.Builder<>(httpHosts, new CustomElasticSearchSinkFunction());
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(new SecuredRestClientFactory("username","password"));
        inputStreamSource.addSink(esSinkBuilder.build());

        try {
            env.execute("event driven job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
