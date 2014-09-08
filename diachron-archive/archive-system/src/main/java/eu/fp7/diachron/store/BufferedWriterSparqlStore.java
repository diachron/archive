package eu.fp7.diachron.store;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * PROTOTYPE (not tested)
 * 
 * Implementation of {@link SparqlStore} which uses an HTTP SPARQL endpoint as data backend.
 * 
 * @author Ruben Navarro Piris
 *
 */
public abstract class BufferedWriterSparqlStore implements SparqlStore {
  private final static Logger LOGGER = LoggerFactory.getLogger(BufferedWriterSparqlStore.class);

  private final int flushSize;

  public BufferedWriterSparqlStore(int flushSize) {
    this.flushSize = flushSize;
  }

  @Override
  public void storeTriples(String targetGraph, Model model) throws IOException {
    LOGGER.trace("Storing {} triples to dataset (graph): {}", model.size(), targetGraph);

    Model buffer = ModelFactory.createDefaultModel();
    StmtIterator iterator = model.listStatements();
    while (iterator.hasNext()) {
      buffer.add(iterator.next());

      if (buffer.size() > flushSize) {
        LOGGER.trace("Flushing buffer of size {}", buffer.size());
        flushWriteBuffer(targetGraph, buffer);
      }
    }
    if (!buffer.isEmpty()) {
      LOGGER.trace("Flushing buffer of size {}", buffer.size());
      flushWriteBuffer(targetGraph, buffer);
    }
    LOGGER
        .trace("Successfully stored {} triples to dataset (graph): {}", model.size(), targetGraph);
  }

  private void flushWriteBuffer(String targetGraph, Model buffer) throws IOException {
    StringWriter out = new StringWriter();
    RDFDataMgr.write(out, buffer, Lang.NTRIPLES);
    this.executeUpdate("INSERT DATA {GRAPH <" + targetGraph + "> { " + out.toString() + "} }");
    buffer.removeAll();
  }

}
