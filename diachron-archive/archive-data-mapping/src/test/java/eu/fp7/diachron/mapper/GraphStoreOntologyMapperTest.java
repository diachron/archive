package eu.fp7.diachron.mapper;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphSimpleMem;
import com.hp.hpl.jena.sparql.modify.GraphStoreBasic;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import eu.fp7.diachron.store.JenaSparqlGraphStore;

/**
 * 
 * @author Ruben Navarro Piris
 *
 */
public class GraphStoreOntologyMapperTest {
  @Test
  public void test() throws IOException {
    DatasetGraph dsg = new DatasetGraphSimpleMem();
    Node graph = new ResourceImpl("http://example.org/versionGraph").asNode();

    // add 3 classes
    Node class1 = new ResourceImpl("http://example.org/class1").asNode();
    Node class2 = new ResourceImpl("http://example.org/class2").asNode();
    Node class3 = new ResourceImpl("http://example.org/class3").asNode();
    dsg.add(graph, class1, RDF.type.asNode(), RDFS.Class.asNode());
    dsg.add(graph, class2, RDF.type.asNode(), OWL.Class.asNode());
    dsg.add(graph, class3, RDFS.subClassOf.asNode(), class1);

    // add 2 properties
    Node property1 = new ResourceImpl("http://example.org/property1").asNode();
    Node property2 = new ResourceImpl("http://example.org/property2").asNode();
    dsg.add(graph, property1, RDF.type.asNode(), RDF.Property.asNode());
    dsg.add(graph, property2, RDFS.subPropertyOf.asNode(), property1);

    // add 2 resources with properties
    Node resource1 = new ResourceImpl("http://example.org/resource1").asNode();
    Node resource2 = new ResourceImpl("http://example.org/resource2").asNode();
    dsg.add(graph, resource1, RDF.type.asNode(), OWL.Thing.asNode());
    dsg.add(graph, resource2, RDFS.label.asNode(), NodeFactory.createLiteral("label"));

    // execute TBox mapping
    GraphStore store = new GraphStoreBasic(dsg);
    RdfDiachronModelMapper mapper =new RdfDiachronModelMapper(new JenaSparqlGraphStore(store, 2), 2);
    mapper.executeTboxMapping(graph.getURI());

    // assert that classes became the new expected types
    checkClass(store, graph, class1);
    checkClass(store, graph, class2);
    checkClass(store, graph, class3);

    // assert that properties became the new expected types
    checkProperty(store, graph, property1);
    checkProperty(store, graph, property2);

    // execute TBox mapping
    mapper.executeAboxMapping(graph.getURI(), true);
    // assert that resources where mapped to records
    // TODO

    // assert that resource properties where mapped to recordAttributes
    // TODO

    // assert dataset mapping
    checkDataset(store, graph);
  }

  private void checkClass(GraphStore store, Node graph, Node clazz) {
    assertTrue(store.contains(graph, clazz, RDF.type.asNode(),
        DiachronVocabulary.ONTOLOGY_CLASS.asNode()));
    assertTrue(store.contains(graph, clazz, RDF.type.asNode(), DiachronVocabulary.CLASS.asNode()));
    assertTrue(store.contains(graph, clazz, RDF.type.asNode(), RDFS.Class.asNode()));
  }

  private void checkProperty(GraphStore store, Node graph, Node property) {
    assertTrue(store.contains(graph, property, RDF.type.asNode(),
        DiachronVocabulary.SCHEMA_PROPERTY.asNode()));
    assertTrue(store.contains(graph, property, RDF.type.asNode(),
        DiachronVocabulary.PROPERTY.asNode()));
    assertTrue(store.contains(graph, property, RDF.type.asNode(),
        DiachronVocabulary.SCHEMA_OBJECT.asNode()));
    assertTrue(store.contains(graph, property, RDF.type.asNode(),
        DiachronVocabulary.ENTITY.asNode()));
  }

  private void checkDataset(GraphStore store, Node graph) {
    assertTrue(store.contains(graph, graph, RDF.type.asNode(), DiachronVocabulary.DATASET.asNode()));
    assertTrue(store.contains(graph, graph, DiachronVocabulary.HAS_RECORD_SET.asNode(), null));
    assertTrue(store.contains(graph, null, DiachronVocabulary.HAS_RECORD.asNode(), null));

  }
}
