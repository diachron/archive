package eu.fp7.diachron.archive.core.store;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * Implementation of {@link SparqlStore} & {@link Loader} which uses a Jena {@link GraphStore} as
 * data backend.
 * 
 * Internally, a {@link SparqlStoreLoader} is used (with the {@link JenaSparqlGraphStore} as
 * {@link SparqlStore} delegate).
 *
 * @author Ruben Navarro Piris
 *
 */
public class JenaSparqlGraphStore implements SparqlStore, Loader {
  private static final Logger LOGGER = LoggerFactory.getLogger(JenaSparqlGraphStore.class);

  private final GraphStore graphStore;
  private final Loader loader;

  public JenaSparqlGraphStore(GraphStore graphStore, int flushSize) {
    this.graphStore = graphStore;
    this.loader = new SparqlStoreLoader(this, flushSize);
  }

  @Override
  public void executeUpdate(String update) throws IOException {
    LOGGER.trace("Executing update: {}", update);
    UpdateExecutionFactory.create(UpdateFactory.create(update), this.graphStore).execute();
  }

  @Override
  public QueryExecution queryExecution(String query) {
    LOGGER.trace("Executing query: {}", query);
    return QueryExecutionFactory.create(query, this.graphStore.toDataset());
  }

  @Override
  public void loadModel(Model model, String namedGraph) throws IOException {
    this.loader.loadModel(model, namedGraph);
  }

  @Override
  public void loadData(InputStream stream, String diachronicDatasetURI) throws IOException {
    this.loader.loadData(stream, diachronicDatasetURI);
  }

}
