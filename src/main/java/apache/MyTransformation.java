package apache;

import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class MyTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
  public static final String FIELDS_CONFIG = "fields";
  private static final String SEPARATOR_CONFIG = "separator";

  private static final String SCHEMA_REGISTRY = "schemaRegistryUrl";

  private List<String> maskedFields;
  private String separator;

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    // FROM:
    // byteArray -> GenericRecord -> record -> walk -> GenericRecord -> byteArray
    // TO:
    // byteArray -> GenericRecord -> walk -> byteArray
    // walk through avro schema
    //
    // limitations:
    // use unions just to make a field nullable
    // leaf must be string
    // silently ignore if types doesn't match
    GenericRecord a = (GenericRecord) record;
    return record;
  }

  private R applyWithSchema(R record) {
    return record;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            FIELDS_CONFIG,
            ConfigDef.Type.LIST,
            ConfigDef.NO_DEFAULT_VALUE,
            new NonEmptyListValidator(),
            ConfigDef.Importance.HIGH,
            "Names of fields to mask.")
        .define(
            SEPARATOR_CONFIG,
            ConfigDef.Type.STRING,
            ".",
            ConfigDef.Importance.HIGH,
            "Separator for navigate nested.")
        .define(
            SCHEMA_REGISTRY,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            "Schema registry url"
        );
  }

  @Override
  public void close() {}

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> map) {
    Map<String, Object> parse = config().parse(map);

    this.maskedFields = (List<String>) parse.get(FIELDS_CONFIG);
    this.separator = (String) parse.get(SEPARATOR_CONFIG);
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R base, Object value);

  public static final class Value<R extends ConnectRecord<R>> extends MyTransformation<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          record.valueSchema(),
          updatedValue,
          record.timestamp(),
          record.headers());
    }
  }
}
