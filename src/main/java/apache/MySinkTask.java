package apache;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static java.nio.file.Files.createTempDirectory;
import static org.apache.iceberg.Files.localOutput;

public class MySinkTask extends SinkTask {
  private class StoredRecords {
    private final long offset;
    private final DataFile dataFile;

    StoredRecords(long offset, DataFile dataFile) {
      this.offset = offset;
      this.dataFile = dataFile;
    }

    public long getOffset() {
      return offset;
    }

    public DataFile getDataFile() {
      return dataFile;
    }
  }

  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
 */
  private static Logger log = LoggerFactory.getLogger(MySinkTask.class);

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get())
  );

  private static final Record RECORD = GenericRecord.create(SCHEMA);

  //private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).bucket("id",5).build();
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private MySinkConnectorConfig config;
  private Path tempDirectory;
  private Table table;
  private ConcurrentLinkedQueue<StoredRecords> storedRecords = new ConcurrentLinkedQueue<>();

  private final FileFormat format = FileFormat.PARQUET;

  @Override
  public void start(Map<String, String> settings) {
    log.error("Starting task with settings {}", settings);
    this.config = new MySinkConnectorConfig(settings);

    try {
      tempDirectory = createTempDirectory("kafka-iceberg-");
      table = new HadoopTables().create(SCHEMA, SPEC, new HashMap<>(), tempDirectory.toString());
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to create Iceberg table.", e);
    }
    log.error("Started connector");

    // TODO load last offset from metadata
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.error("Putting {} records", records.size());
    if (!records.isEmpty()) {
      File outputFile = new File(tempDirectory.toFile(), format.addExtension(UUID.randomUUID().toString()));

      List<Record> recordsToWrite = records.stream().map((r) -> {
        Record record = RECORD.copy();
        record.setField("id", r.kafkaOffset());
        record.setField("data", "data");
        return record;
      }).collect(Collectors.toList());

      long lastOffset = (long) recordsToWrite.get(recordsToWrite.size()-1).getField("id");

      try {
        DataFile dataFile = writeFile(outputFile, SCHEMA, recordsToWrite);
        storedRecords.add(new StoredRecords(lastOffset, dataFile));
      } catch (IOException e) {
        log.error("Failed to write records in Iceberg",e);
        throw new IllegalStateException("Failed to write records in Iceberg", e);
      }
    } else {
      log.error("records collection are empty");
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // TODO save offset in metadata
    if (map == null || map.size() == 0) {
      return;
    }

    if (map.size() > 1) {
      throw new IllegalArgumentException("More than one partition");
    }

    OffsetAndMetadata offsetAndMetadata = map.values().iterator().next();
    long offsetToCommit = offsetAndMetadata.offset();

    AppendFiles appendFiles = table.newAppend();
    int addedFiles = 0;

    while (true) {
      StoredRecords headRecord = storedRecords.peek();
      if (headRecord != null) {
        if (headRecord.offset <= offsetToCommit) {
          appendFiles = appendFiles.appendFile(headRecord.dataFile);
          storedRecords.poll(); // used record
          addedFiles++;
        } else {
          break;
        }
      } else {
        break;
      }
    }

    if (addedFiles > 0) {
      appendFiles.commit();
    }
  }

  @Override
  public void stop() {
    // Close resources here.
    // TODO
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  private DataFile writeFile(File output, Schema schema, List<Record> records) throws IOException {
    OutputFile outputFile = localOutput(output);

    FileAppender<Record> parquetAppender = Parquet.write(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build();
    try {
      parquetAppender.addAll(records);
    } finally {
      parquetAppender.close();
    }

    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(outputFile.toInputFile())
        .withMetrics(parquetAppender.metrics())
        .build();
  }
}

