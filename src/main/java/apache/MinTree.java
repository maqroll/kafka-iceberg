package apache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;

public class MinTree {
  private static final Pattern REGEXP =
      Pattern.compile("(?<field>[^\\[\\]]*)(\\[((?<idx>\\*|[0-9]*)|('(?<key>.*)'))\\])?");
  private final Map<String, Node> roots = new HashMap<>();

  private Node newNode(String ref, boolean leaf) {
    Matcher matcher = REGEXP.matcher(ref);
    Node node = null;

    if (matcher.matches()) {
      String field = matcher.group("field");
      String key = matcher.group("key");
      String idx = matcher.group("idx");

      if (key != null) { // map
        if (leaf) {
          node = new Node(field, NodeType.MAP_LEAF, key);
        } else {
          node = new Node(field, NodeType.MAP, key);
        }
      } else if (idx != null) { // array
        if (leaf) {
          node = new Node(field, NodeType.ARRAY_LEAF, idx);
        } else {
          node = new Node(field, NodeType.ARRAY, idx);
        }
      } else if (leaf) {
        node = new Node(field, NodeType.LEAF, null);
      } else {
        node = new Node(field, NodeType.STRUCT, null);
      }
    } else {
      throw new IllegalArgumentException("Failed to parse " + ref + " as node");
    }

    return node;
  }

  public MinTree(List<String> refs, String separator) {
    List<String> sorted = refs.stream().sorted().collect(Collectors.toList());

    String secureSeparator = separator.equals(".") ? "\\." : separator;

    for (String ref : sorted) {
      String[] split = ref.contains(separator) ? ref.split(secureSeparator) : new String[] {ref};

      Map<String, Node> current = roots;

      for (int i = 0; i < split.length; i++) {
        final boolean leaf = (i == split.length - 1);

        current = current.computeIfAbsent(split[i], k -> newNode(k, leaf)).children();
      }
    }
  }

  private void visitInOrderInner(
      BiFunction<Node, List<Struct>, List<Struct>> function, Node node, List<Struct> parent) {
    List<Struct> current = function.apply(node, parent);

    if (!current.isEmpty()) {
      for (Node child : node.nodes()) {
        visitInOrderInner(function, child, current);
      }
    }
  }

  private CharSequence hash(CharSequence input) {
    return "hash";
  }

  // https://stackoverflow.com/questions/34070028/get-a-typed-value-from-an-avro-genericrecord/34234039
  private void visitInOrderInner(List<Object> current, Node node/*, Schema currentSchema*/) {
    List<Object> next = new ArrayList<>();

    // doesn't rely on schema types BUT in actual types (standard mapping)
    switch (node.type()) {
      case MAP:
        for(Object o: current) {
          if (o instanceof GenericRecord) {
            GenericRecord rec = (GenericRecord) o;
            if (rec.hasField(node.getId()) && rec.get(node.getId()) instanceof Map) {
              HashMap map = (HashMap) rec.get(node.getId());
              next.addAll(map.values());
            }
          }
        }
        break;
      case ARRAY:
        for(Object o: current) {
          if (o instanceof GenericRecord) {
            GenericRecord rec = (GenericRecord) o;
            if (rec.hasField(node.getId()) && rec.get(node.getId()) instanceof GenericData.Array) {
              GenericData.Array arr = (GenericData.Array) rec.get(node.getId());
              next.addAll(arr);
            }
          }
        }
        break;
      case STRUCT:
        for(Object o: current) {
          if (o instanceof GenericRecord) {
            GenericRecord rec = (GenericRecord) o;
            if (rec.hasField(node.getId()) && rec.get(node.getId()) instanceof GenericRecord) {
              next.add(rec.get(node.getId()));
            }
          }
        }
        break;
      case LEAF:
        for(Object o: current) {
          if (o instanceof GenericRecord) {
            GenericRecord rec = (GenericRecord) o;
            if (rec.hasField(node.getId()) && rec.get(node.getId()) instanceof CharSequence) {
              rec.put(node.getId(), hash((CharSequence) rec.get(node.getId())));
            }
          }
        }
        break;
      case MAP_LEAF:
        for(Object o: current) {
          if (o instanceof GenericRecord) {
            GenericRecord rec = (GenericRecord) o;
            if (rec.hasField(node.getId()) && rec.get(node.getId()) instanceof Map) {
              HashMap map = (HashMap) rec.get(node.getId());
              if (node.filter().equals("*")) {
                for (Object v: map.keySet()) {
                  Object val = map.get(v);
                  if (val instanceof CharSequence) {
                    map.put(v, hash((CharSequence) val));
                  }
                }
              } else {
                Object val = map.get(node.filter());
                if (val != null && val instanceof CharSequence) {
                  map.put(node.filter(), hash((CharSequence) val));
                }
              }
            }
          }
        }
        break;
      case ARRAY_LEAF:
        for(Object o: current) {
          if (o instanceof GenericRecord) {
            GenericRecord rec = (GenericRecord) o;
            if (rec.hasField(node.getId()) && rec.get(node.getId()) instanceof List) {
              List arr = (List) rec.get(node.getId());
              if (node.filter().equals("*")) {
                for (int i=0; i<arr.size(); i++) {
                  Object v = arr.get(i);
                  if (v instanceof CharSequence) {
                    arr.set(i, hash((CharSequence) v));
                  }
                }
              } else {
                Integer idx = Integer.valueOf(node.filter());
                if (idx < arr.size()) {
                  Object val = arr.get(idx);
                  if (val != null && val instanceof CharSequence) {
                    arr.set(idx, hash((CharSequence) val));
                  }
                }
              }
            }
          }
        }
        break;
    }

    if (!next.isEmpty()) {
      for (Node child : node.nodes()) {
        visitInOrderInner(next,child);
      }
    }
  }

  public void visitInOrder(GenericRecord record) {
    List<Node> nodes = new ArrayList<>(roots.values());

    for (Node node : nodes) {
      if (record.hasField(node.getId())) {
        visitInOrderInner(Collections.singletonList(record),
            node/*,
            schema*/);
      }
    }
  }
}

