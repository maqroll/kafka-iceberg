package apache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

public class AvroTest {

  @Test
  public void simpleTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is "
        + "enrolled in marketing emails\" }\n"
        +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("first_name", "last_name"), ".");

    minTree.visitInOrder(myCustomer);

    assertThat(myCustomer.get("first_name")).isEqualTo("hash");
    assertThat(myCustomer.get("last_name")).isEqualTo("hash");
  }

  @Test
  public void missingTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is "
        + "enrolled in marketing emails\" }\n"
        +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("a.b.c.d.e.f"), ".");

    minTree.visitInOrder(myCustomer);

    assertThat(myCustomer.get("first_name")).isEqualTo("John");
    assertThat(myCustomer.get("last_name")).isEqualTo("Doe");
  }

  @Test
  public void presentButInvalidTypeTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is "
        + "enrolled in marketing emails\" }\n"
        +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("age", "height", "weight"), ".");

    minTree.visitInOrder(myCustomer);

    assertThat(myCustomer.get("age")).isEqualTo(Integer.valueOf(26));
    assertThat(myCustomer.get("height")).isEqualTo(Float.valueOf(175f));
    assertThat(myCustomer.get("weight")).isEqualTo(Float.valueOf(70.5f));
  }

  @Test
  public void nestedTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" },"
        + "{ \"name\": \"nested\","
        + "            \"type\": {\n"
        + "              \"type\": \"record\",\n"
        + "              \"name\": \"nestedType\",\n"
        + "              \"fields\": [\n"
        + "                {\n"
        + "                  \"name\": \"id\",\n"
        + "                  \"type\": \"string\"\n"
        + "                },\n"
        + "                {\n"
        + "                  \"name\": \"store_id\",\n"
        + "                  \"type\": \"string\"\n"
        + "                },\n"
        + "                {\n"
        + "                  \"name\": \"datetime\",\n"
        + "                  \"type\": \"long\"\n"
        + "                }\n"
        + "              ]\n"
        + "            }}\n\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);

    GenericRecordBuilder nestedBuilder = new GenericRecordBuilder(schema.getField("nested").schema());
    nestedBuilder.set("id", "id");
    nestedBuilder.set("store_id", "store_id");
    nestedBuilder.set("datetime", 1234l);
    Record nested = nestedBuilder.build();

    customerBuilder.set("nested", nested);

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("nested.id", "nested.missing", "nested.datetime"), ".");

    minTree.visitInOrder(myCustomer);

    nested = (Record) myCustomer.get("nested");

    assertThat(nested.get("id")).isEqualTo("hash");
    assertThat(nested.get("store_id")).isEqualTo("store_id");
    assertThat(nested.get("datetime")).isEqualTo(Long.valueOf(1234l));
  }

  @Test
  public void arrayTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" },"
        +
        "        { \"name\": \"arr\", \"type\": { \"type\": \"array\",\n"
        + "        \"items\": {\n"
        + "          \"name\": \"Item\",\n"
        + "          \"type\": \"string\"}}}\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    customerBuilder.set("arr", Arrays.asList("1", "2", "3"));

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("arr[*]"), ".");

    minTree.visitInOrder(myCustomer);

    Collection arr = (Collection) myCustomer.get("arr");

    assertThat(arr).isNotNull();
    arr.stream().forEach(o -> assertThat(o).isEqualTo("hash"));
  }

  @Test
  public void arrayIndexTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" },"
        +
        "        { \"name\": \"arr\", \"type\": { \"type\": \"array\",\n"
        + "        \"items\": {\n"
        + "          \"name\": \"Item\",\n"
        + "          \"type\": \"string\"}}}\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    customerBuilder.set("arr", Arrays.asList("1", "2", "3"));

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("arr[1]"), ".");

    minTree.visitInOrder(myCustomer);

    List arr = (List) myCustomer.get("arr");

    assertThat(arr).isNotNull();
    assertThat(arr.get(0)).isEqualTo("1");
    assertThat(arr.get(1)).isEqualTo("hash");
    assertThat(arr.get(2)).isEqualTo("3");
  }

  @Test
  public void arrayOverflowIndexTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" },"
        +
        "        { \"name\": \"arr\", \"type\": { \"type\": \"array\",\n"
        + "        \"items\": {\n"
        + "          \"name\": \"Item\",\n"
        + "          \"type\": \"string\"}}}\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", "Doe");
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    customerBuilder.set("arr", Arrays.asList("1", "2", "3"));

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("arr[10]"), ".");

    minTree.visitInOrder(myCustomer);

    List arr = (List) myCustomer.get("arr");

    assertThat(arr).isNotNull();
    assertThat(arr.get(0)).isEqualTo("1");
    assertThat(arr.get(1)).isEqualTo("2");
    assertThat(arr.get(2)).isEqualTo("3");
  }


  @Test
  public void nullTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": [ \"null\", \"string\"], \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" },"
        +
        "        { \"name\": \"arr\", \"type\": { \"type\": \"array\",\n"
        + "        \"items\": {\n"
        + "          \"name\": \"Item\",\n"
        + "          \"type\": \"string\"}}}\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", null);
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    customerBuilder.set("automated_email", false);
    customerBuilder.set("arr", Arrays.asList("1", "2", "3"));

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("last_name"), ".");

    minTree.visitInOrder(myCustomer);

    assertThat(myCustomer.get("last_name")).isNull();
  }

  @Test
  public void mapTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": [ \"null\", \"string\"], \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"tags\", \"type\": { \"type\": \"map\", \"values\": \"string\" }}\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", null);
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    Map<String,String> tags = new HashMap<>();
    tags.put("a","a");
    tags.put("b","b");
    customerBuilder.set("tags", tags);

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("tags['*']"), ".");

    minTree.visitInOrder(myCustomer);

    assertThat( (Map<String,String>) myCustomer.get("tags")).allSatisfy((k, v) -> v.equals("hash"));
  }

  @Test
  public void mapWithKeyTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": [ \"null\", \"string\"], \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"tags\", \"type\": { \"type\": \"map\", \"values\": \"string\" }}\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", null);
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    Map<String,String> tags = new HashMap<>();
    tags.put("a","a");
    tags.put("b","b");
    customerBuilder.set("tags", tags);

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("tags['a']"), ".");

    minTree.visitInOrder(myCustomer);

    assertThat( (Map<String,String>) myCustomer.get("tags")).containsExactly(new SimpleEntry<>("a","hash"),new SimpleEntry<>("b","b"));
  }

  @Test
  public void mapWithMissingKeyTest() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse("{\n" +
        "     \"type\": \"record\",\n" +
        "     \"namespace\": \"com.example\",\n" +
        "     \"name\": \"Customer\",\n" +
        "     \"fields\": [\n" +
        "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
        "       { \"name\": \"last_name\", \"type\": [ \"null\", \"string\"], \"doc\": \"Last Name of Customer\" },\n" +
        "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
        "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
        "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
        "       { \"name\": \"tags\", \"type\": { \"type\": \"map\", \"values\": \"string\" }}\n" +
        "     ]\n" +
        "}");

    GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
    customerBuilder.set("first_name", "John");
    customerBuilder.set("last_name", null);
    customerBuilder.set("age", 26);
    customerBuilder.set("height", 175f);
    customerBuilder.set("weight", 70.5f);
    Map<String,String> tags = new HashMap<>();
    tags.put("a","a");
    tags.put("b","b");
    customerBuilder.set("tags", tags);

    GenericRecord myCustomer = customerBuilder.build();

    MinTree minTree = new MinTree(Arrays.asList("tags['d']"), ".");

    minTree.visitInOrder(myCustomer);

    assertThat( (Map<String,String>) myCustomer.get("tags")).containsExactly(new SimpleEntry<>("a","a"),new SimpleEntry<>("b","b"));
  }
}
