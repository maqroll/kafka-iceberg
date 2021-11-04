package apache;

import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * This transformation deliberately ignores the KC model (converter -> transformation -> converter).
 *
 * Of course it can't be reused across different input formats.
 *
 * Written to explore a different approach to handle transformations over avro messages with very specific requirements (input and
 * output schemas must be EXACTLY the same).
 * 
 * @param <R>
 */
public abstract class MyTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
  public static final String FIELDS_CONFIG = "fields";
  private static final String SEPARATOR_CONFIG = "separator";

  private static final String SCHEMA_REGISTRY = "schemaRegistryUrl";

  private List<String> maskedFields;
  private String separator;

  private String schemaRegistry;

  private Serde<GenericRecord> genericSerde;

  private MinTree minTree;

  @Override
  public R apply(R record) {
    // assume schemaless
    return applySchemaless(record);
  }

  private R applySchemaless(R record) {
    // FROM:
    // byteArray -> GenericRecord -> record -> walk -> GenericRecord -> byteArray
    // TO:
    // byteArray -> GenericRecord -> walk -> byteArray
    // walk through avro schema
    //
    // limitations:
    // leaf must be string
    // silently ignore if types doesn't match
    byte[] value = (byte[]) record.value();

    GenericRecord avroRecord = genericSerde.deserializer().deserialize(record.topic(), value);

    minTree.visitInOrder(avroRecord);

    // don't generate new schemas!!
    byte[] output = genericSerde.serializer().serialize(record.topic(), avroRecord);

    return newRecord(record,output);
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
    this.schemaRegistry = (String) parse.get(SCHEMA_REGISTRY);

    genericSerde = AvroSerdes.Generic(this.schemaRegistry, false);

    minTree = new MinTree(maskedFields, separator);
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
