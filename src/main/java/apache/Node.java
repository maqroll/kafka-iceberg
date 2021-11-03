package apache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Node {
  private final NodeType type;
  private final String filter;
  private Map<String, Node> children = new HashMap<>();
  private final String id;

  public Node(String id, NodeType type, String filter) {
    this.id = id;
    this.type = type;
    this.filter = filter;
  }

  public Node addNode(Node child) {
    children.put(child.getId(), child);
    return this;
  }

  public NodeType type() {
    return type;
  }

  public String filter() {
    return filter;
  }

  public Map<String, Node> children() {
    return children;
  }

  public List<Node> nodes() {
    return children.values().stream().collect(Collectors.toList());
  }

  public Node getChild(String id) {
    return children.get(id);
  }

  public String getId() {
    return id;
  }
}

