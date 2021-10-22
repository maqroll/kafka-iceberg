package apache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

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
