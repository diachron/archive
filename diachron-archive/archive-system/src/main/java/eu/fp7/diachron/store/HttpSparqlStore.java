package eu.fp7.diachron.store;

import java.io.IOException;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * PROTOTYPE (not tested)
 * 
 * Implementation of {@link SparqlStore} which uses an HTTP SPARQL endpoint as data backend.
 * 
 * @author Ruben Navarro Piris
 *
 */
public class HttpSparqlStore extends BufferedWriterSparqlStore implements SparqlStore {

  private final String endpoint;

  public HttpSparqlStore(String endpoint, int flushSize) {
    super(flushSize);
    this.endpoint = endpoint;
  }

  @Override
  public void executeUpdate(String update) throws IOException {
    UpdateExecutionFactory.createRemote(UpdateFactory.create(update), this.endpoint).execute();
  }

  @Override
  public QueryExecution queryExecution(String query) {
    return QueryExecutionFactory.sparqlService(this.endpoint, query);
  }

}
