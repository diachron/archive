package eu.fp7.diachron.store;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * Implementation of {@link SparqlStore} which uses a Jena {@link GraphStore} as data backend.
 *
 * @author Ruben Navarro Piris
 *
 */
public class JenaSparqlGraphStore extends BufferedWriterSparqlStore implements SparqlStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(JenaSparqlGraphStore.class);

  private final GraphStore graphStore;

  public JenaSparqlGraphStore(GraphStore graphStore, int flushSize) {
    super(flushSize);
    this.graphStore = graphStore;
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

}
