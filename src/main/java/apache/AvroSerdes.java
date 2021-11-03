package apache;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

public class AvroSerdes {
  public static Serde<GenericRecord> Generic(String url, boolean isKey) {
    Map<String, String> serdeConfig = new HashMap<>();
    serdeConfig.put("schema.registry.url", url);
    serdeConfig.put("schema.registry.basic.auth.credentials.source", "URL");

    Serde<GenericRecord> serde = new GenericAvroSerde();
    serde.configure(serdeConfig, isKey);
    return serde;
  }
}