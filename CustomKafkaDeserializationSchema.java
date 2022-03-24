import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;


@Slf4j
public class CustomKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<Tuple2<String,String>> {
    @Override
    public TypeInformation<Tuple2<String,String>> getProducedType() {
        return new TupleTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tuple2<String,String>> out) throws IOException {
        out.collect(new Tuple2<>(record.topic(), new String(record.value(), "UTF-8")));
    }
}

