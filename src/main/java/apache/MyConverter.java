package apache;

import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyConverter implements Converter {
  private static Logger log = LoggerFactory.getLogger(MyConverter.class);

  @Override
  public void configure(Map<String, ?> settings, boolean isKey) {
    //TODO: Do your setup here.
  }

  @Override
  public byte[] fromConnectData(String s, Schema schema, Object o) {
    throw new UnsupportedOperationException(
        "This needs to be completed"
    );
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    throw new UnsupportedOperationException(
        "This converter requires Kafka 2.4.0 or higher with header support."
    );
  }

  @Override
  public SchemaAndValue toConnectData(String s, byte[] bytes) {
    throw new UnsupportedOperationException(
        "This needs to be completed"
    );
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    throw new UnsupportedOperationException(
        "This converter requires Kafka 2.4.0 or higher with header support."
    );
  }
}