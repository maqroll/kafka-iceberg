package apache;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MySinkTaskTest {
  @Test
  public void test() {
    MySinkTask mySinkTask = new MySinkTask();
    mySinkTask.start(new HashMap<>());
    List<SinkRecord> records = new ArrayList<>();
    records.add(new SinkRecord("topic", 0, null, null, null, null, 0));
    mySinkTask.put(records);
  }
}
