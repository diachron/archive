package eu.fp7.diachron.archive.core.store;

import java.io.IOException;

import com.hp.hpl.jena.query.QueryExecution;

/**
 * Defines basic SPARQL interaction functionalities. Read functionalities are based on the
 * {@link QueryExecution} API of Jena.
 * 
 * @author Ruben Navarro Piris
 *
 */
public interface SparqlStore {

  /**
   * Executes a SPARQL 1.1 update query
   * 
   * @param update
   * @throws IOException
   */
  void executeUpdate(String update) throws IOException;

  /**
   * Creates a SPARQL 1.1 query execution.
   * 
   * Clients of this API must call the {@link QueryExecution#close()} method once processing is done
   * (at best in a finally block to ensure its execution), otherwise connection/memory leaks could
   * appear.
   * 
   * Implementations of this API must ensure that on execution of {@link QueryExecution#close()} all
   * statement and/or connection resources are closed.
   * 
   * @param query
   * @return
   * @throws IOException
   */
  QueryExecution queryExecution(String query);

}
