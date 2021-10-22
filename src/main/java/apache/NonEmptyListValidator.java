package apache;

import java.util.List;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class NonEmptyListValidator implements ConfigDef.Validator {

  @Override
  public void ensureValid(String name, Object value) {
    if (value == null || ((List) value).isEmpty()) {
      throw new ConfigException(name, value, "Empty list");
    }
  }

  @Override
  public String toString() {
    return "non-empty list";
  }
}
