package eu.fp7.diachron.archive.mapping;

import static eu.fp7.diachron.archive.mapping.MappingUtils.sha256;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.XSD;

import eu.fp7.diachron.archive.core.store.Loader;
import eu.fp7.diachron.archive.core.store.SparqlStore;

/**
 * Implementation of the {@link DiachronModelMapper} interface. Reads & inserts data using solely
 * SPARQL queries. Extensions of this class are responsible of defining query/update access, the
 * rest of the mapping logic is provided by this class.
 * 
 * @author Ruben Navarro Piris
 *
 */
public final class RdfDiachronModelMapper implements DiachronModelMapper {
  private final static Logger LOGGER = LoggerFactory.getLogger(RdfDiachronModelMapper.class);

  // FIXME these prefixes should be configured externally
  private static final String DIACHRON_RECORD = "http://example.org/record/";
  private static final String DIACHRON_ATTRIBUTE = "http://example.org/attribute/";

  // @formatter:off
  private static final String SPARQL_PREFIXES = 
      "PREFIX diachron: <" + DiachronVocabulary.DIACHRON_PREFIX + "> "
      + "PREFIX xsd: <" + XSD.getURI() + "> "
      + "PREFIX rdf: <" + RDF.getURI() + "> "
      + "PREFIX rdfs: <" + RDFS.getURI() + "> "
      + "PREFIX owl: <" + OWL.getURI() + "> ";
  // @formatter:on

  private static final Set<String> classGraphPatterns = Sets.newHashSet("?s rdf:type rdfs:Class",
      "?s rdf:type owl:Class", "?s rdfs:subClassOf ?sc");
  private static final Set<String> propertyGraphPatterns = Sets.newHashSet(
      "?s rdf:type rdf:Property", "?s rdfs:subPropertyOf ?sp");

  // class mapping
  /*
   * FIXME the actually required types have to be checked
   */
  private static final String classMappingUpdate =
      SPARQL_PREFIXES
          + "INSERT {GRAPH <%s> {?s rdf:type diachron:OntologyClass, diachron:Class, rdfs:Class}} WHERE { GRAPH <%s> { "
          + new StringBuilder("{").append(Joiner.on("} UNION {").join(classGraphPatterns))
              .append("}").toString() + "} }";

  // property mapping
  /*
   * FIXME the actually required types have to be checked
   */
  private static final String propertyMappingUpdate =
      SPARQL_PREFIXES
          + "INSERT {GRAPH <%s> {?s rdf:type diachron:OntologyProperty, diachron:Property, diachron:SchemaProperty, diachron:SchemaObject, diachron:Entity}} WHERE { GRAPH <%s> { "
          + new StringBuilder("{").append(Joiner.on("} UNION {").join(propertyGraphPatterns))
              .append("}").toString() + "} }";

  // class & property filtering
  private String filterPropertiesAndClasses = new StringBuilder("FILTER NOT EXISTS {")
      .append(
          Joiner.on("} FILTER NOT EXISTS {").join(
              ImmutableSet.<String>builder().addAll(classGraphPatterns)
                  .addAll(propertyGraphPatterns).build())).append("}").toString();

  private final SparqlStore store;
  private final Loader loader;
  private final long flushSize;

  /**
   * Standard constructor. In standard case the {@link SparqlStore} & {@link Loader} point to the
   * same data backend (since the mapping information should be stored in the same location). Using
   * different locations implies that the original data & its diachron mapping are separated (which
   * is not generally supported in the diachron framework), so use with caution.
   * 
   * @param store used to execute the sparql queries (selection)
   * @param loader used to store the information
   * @param flushSize
   */
  public RdfDiachronModelMapper(SparqlStore store, Loader loader, long flushSize) {
    this.store = Preconditions.checkNotNull(store);
    this.loader = Preconditions.checkNotNull(loader);
    this.flushSize = flushSize;
  }

  @Override
  public void executeTboxMapping(String datasetUri) throws IOException {
    this.store.executeUpdate(String.format(classMappingUpdate, datasetUri, datasetUri));
    this.store.executeUpdate(String.format(propertyMappingUpdate, datasetUri, datasetUri));
  }

  @Override
  public void executeAboxMapping(String datasetUri, boolean mapRecordset) throws IOException {
    /**
     * Records and attributes mapping
     */
    LOGGER.debug("Executing abox mapping for dataset: {}", datasetUri);

    /*
     * sorting on subject is critical for the record & record attribute mapping (see below). Do not
     * deactivate!
     */
    String query =
        String.format(SPARQL_PREFIXES + "SELECT ?s ?p ?o WHERE { GRAPH <%s> { ?s ?p ?o . "
            + filterPropertiesAndClasses + " } } ORDER BY ?s", datasetUri);
    LOGGER.debug("Executing query for record retrieval: {}", query);
    QueryExecution queryExecution = this.store.queryExecution(query);
    try {
      ResultSet rs = queryExecution.execSelect();
      Resource currentSubject = null;
      Resource currentRecordId = null;

      Model buffer = ModelFactory.createDefaultModel();
      int recordIdCount = 0;
      while (rs.hasNext()) {
        QuerySolution sol = rs.next();

        /*
         * next subject (variable s)-> new currentRecordId
         */
        if (!sol.getResource("s").equals(currentSubject)) {
          currentSubject = sol.getResource("s");

          currentRecordId =
              new ResourceImpl(DIACHRON_RECORD.concat(sha256(currentSubject.asNode())));
          buffer.add(currentRecordId, RDF.type, DiachronVocabulary.RECORD.asResource());
          buffer.add(currentRecordId, RDF.type, DiachronVocabulary.DATA_OBJECT.asResource());
          buffer.add(currentRecordId, RDF.type, DiachronVocabulary.ENTITY.asResource());
          buffer.add(currentRecordId, DiachronVocabulary.SUBJECT.asProperty(), currentSubject);

          recordIdCount++;
        } // no else

        /*
         * Attribute mapped to current subject (record), correct record id guaranteed by query
         * sorting on subject & previous step
         */
        Resource recordAttributeId =
            new ResourceImpl(DIACHRON_ATTRIBUTE.concat(sha256(currentSubject.asNode(), sol.get("p")
                .asNode(), sol.get("o").asNode())));
        buffer.add(recordAttributeId, RDF.type, DiachronVocabulary.RECORD_ATTRIBUTE.asResource());
        buffer.add(recordAttributeId, RDF.type, DiachronVocabulary.DATA_OBJECT.asResource());
        buffer.add(recordAttributeId, RDF.type, DiachronVocabulary.ENTITY.asResource());
        buffer.add(recordAttributeId, DiachronVocabulary.PREDICATE.asProperty(), sol.get("p"));
        buffer.add(recordAttributeId, DiachronVocabulary.OBJECT.asProperty(), sol.get("o"));
        buffer.add(currentRecordId, DiachronVocabulary.HAS_ATTRIBUTE.asProperty(),
            recordAttributeId);
        // FIXME this line is actually wrong, but necessary for compatibility reasons with the
        // change detector of Forth
        buffer.add(recordAttributeId, DiachronVocabulary.PROPERTY.asProperty(), sol.get("p"));

        if (buffer.size() >= flushSize) {
          this.loader.loadModel(buffer, datasetUri);
          buffer.removeAll();
        }
      }

      if (!buffer.isEmpty()) {
        this.loader.loadModel(buffer, datasetUri);
      }

      LOGGER.debug("{} records mapped for dataset: {}", recordIdCount, datasetUri);
    } finally {
      queryExecution.close();
    }

    /**
     * Dataset mapping (if requested)
     */
    if (mapRecordset) {

      LOGGER.debug("Executing explicit recordset mapping to dataset: {}", datasetUri);

      Model mapping = ModelFactory.createDefaultModel();
      mapping.add(new ResourceImpl(datasetUri), RDF.type, DiachronVocabulary.DATASET.asResource());
      String recordSetUri = datasetUri.concat("/recordSet");
      this.store.executeUpdate(SPARQL_PREFIXES + "insert {GRAPH <" + datasetUri + "> {<"
          + datasetUri + "> rdf:type diachron:Dataset . <" + datasetUri
          + "> diachron:hasRecordSet <" + recordSetUri + ">. <" + recordSetUri
          + "> rdf:type diachron:RecordSet . <" + recordSetUri
          + "> diachron:hasRecord ?r . }} where {GRAPH <" + datasetUri
          + "> {?r a diachron:Record .}}");
    }
  }

}
