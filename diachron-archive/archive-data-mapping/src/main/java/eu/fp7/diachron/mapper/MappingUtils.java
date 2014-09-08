package eu.fp7.diachron.mapper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.commons.codec.binary.Hex;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.util.FmtUtils;

/**
 * 
 * @author Ruben Navarro Piris
 *
 */
public class MappingUtils {

  private static MessageDigest sha256;
  static {
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(
          "Since the algorithm is statically defined, this should never happen!", e);
    }
  }

  /**
   * 
   * @param nodes
   * @return
   */
  public static String sha256(Node... nodes) {
    // FIXME ensure correct serialization
    List<String> serializedNodes = Lists.newArrayList();
    for (Node node : nodes) {
      serializedNodes.add(sparqlTerm(node));
    }
    String serializedNodesString = Joiner.on(" ").join(serializedNodes);
    return Hex.encodeHexString(sha256.digest(serializedNodesString.getBytes()));
  }

  public static String sparqlTerm(Node node) {
    return FmtUtils.stringForNode(node);
  }


}
