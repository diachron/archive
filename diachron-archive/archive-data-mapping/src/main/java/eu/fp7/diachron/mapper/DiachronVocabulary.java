package eu.fp7.diachron.mapper;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

/**
 * @author Simon Jupp
 * @date 10/02/2014 Functional Genomics Group EMBL-EBI
 */
public enum DiachronVocabulary {

  /*
   * FIXME at some points in the specifications, different names for equivalent concepts were
   * mentioned. This should be cleared
   */

  // Classes
  // @formatter:off
  ENTITY("Entity"), 
  SCHEMA_OBJECT("SchemaObject"), 
  DATA_OBJECT("DataObject"), 
  CLASS("Class"), ONTOLOGY_CLASS("OntologyClass"), // FIXME
  PROPERTY("Property"), SCHEMA_PROPERTY("SchemaProperty"), // FIXME
  LITERAL("Literal"),
  RECORD("Record"),
  RECORD_ATTRIBUTE("RecordAttribute"),
  RESOURCE("Resource"),
  SET("Set"), 
  SCHEMA_SET("SchemaSet"), 
  RECORD_SET("RecordSet"), 
  RESOURCE_SET("ResourceSet"), 
  DATASET("Dataset"), 
  DIACHRONIC_DATASET("DiachronicDataset"),

  // Properties
  HAS_SCHEMA_SET("hasSchemaSet"), 
  HAS_INSTANTIATION("hasInstantiation"), 
  HAS_RECORD_SET("hasRecordSet"),
  HAS_ATTRIBUTE("hasAttribute"),  HAS_RECORD_ATTRIBUTE("hasRecordAttribute"), // FIXME
  HAS_RECORD("hasRecord"), 
  HAS_PART("hasPart"), 
  SUBJECT("subject"), 
  PREDICATE("predicate"), 
  OBJECT("object");
  // @formatter:on

  public final static String DIACHRON_PREFIX = "http://www.diachron-fp7.eu/ontology#";
  public final String uri;

  private DiachronVocabulary(final String name) {
    this.uri = DIACHRON_PREFIX.concat(name);
  }

  public String getURI() {
    return uri;
  }

  public Resource asResource() {
    return ResourceFactory.createResource(uri);
  }

  public Property asProperty() {
    return ResourceFactory.createProperty(uri);
  }

  public Node asNode() {
    return NodeFactory.createURI(uri);
  }
}
