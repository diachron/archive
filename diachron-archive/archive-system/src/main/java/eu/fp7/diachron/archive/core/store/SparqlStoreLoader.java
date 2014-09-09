package eu.fp7.diachron.archive.core.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Implementation of {@link Loader}
 * 
 * @author Ruben Navarro Piris
 *
 */
public class SparqlStoreLoader implements Loader {
  private final static Logger LOGGER = LoggerFactory.getLogger(SparqlStoreLoader.class);

  private final SparqlStore sparqlStore;
  private final int flushSize;

  public SparqlStoreLoader(SparqlStore sparqlStore, int flushSize) {
    this.sparqlStore = sparqlStore;
    this.flushSize = flushSize;
  }

  @Override
  public void loadModel(Model model, String namedGraph) throws IOException {
    LOGGER.trace("Storing {} triples to dataset (graph): {}", model.size(), namedGraph);

    Model buffer = ModelFactory.createDefaultModel();
    StmtIterator iterator = model.listStatements();
    while (iterator.hasNext()) {
      buffer.add(iterator.next());

      if (buffer.size() > flushSize) {
        LOGGER.trace("Flushing buffer of size {}", buffer.size());
        flushWriteBuffer(namedGraph, buffer);
      }
    }
    if (!buffer.isEmpty()) {
      LOGGER.trace("Flushing buffer of size {}", buffer.size());
      flushWriteBuffer(namedGraph, buffer);
    }
    LOGGER.trace("Successfully stored {} triples to dataset (graph): {}", model.size(), namedGraph);
  }

  @Override
  public void loadData(InputStream stream, String diachronicDatasetURI) {
    /*
     * TODO streaming approach: parse the stream until a buffer of size flushSize is generated,
     * flush it,repeat until stream fully parsed & stored
     */
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private void flushWriteBuffer(String targetGraph, Model buffer) throws IOException {
    StringWriter out = new StringWriter();
    RDFDataMgr.write(out, buffer, Lang.NTRIPLES);
    this.sparqlStore.executeUpdate("INSERT DATA {GRAPH <" + targetGraph + "> { " + out.toString()
        + "} }");
    buffer.removeAll();
  }

}
