package eu.fp7.diachron.archive.core.store;


import java.io.Closeable;
import java.io.IOException;

import virtuoso.jdbc4.VirtuosoDataSource;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

import com.hp.hpl.jena.query.QueryExecution;

/**
 * 
 * Implementation of {@link SparqlStore} which uses an OpenLink Virtuoso server as data backend.
 *
 * @author Ruben Navarro Piris
 *
 */
public class VirtuosoSparqlStore implements SparqlStore {

  private final VirtuosoDataSource dataSource;

  public VirtuosoSparqlStore(VirtuosoDataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void executeUpdate(String update) throws IOException {
    VirtGraph virtGraph = new VirtGraph(this.dataSource);
    /*
     * TODO we discovered that there is (or was) a bug in Virtuoso when inserting information on
     * graphs which were not explicitly previously created (with a 'CREATE GRAPH X' statement).
     * Since standard SPARQL accepts inserting directly into a new graph (which should be implicitly
     * created), we could identify to which graphs is information being inserted and execute a
     * 'CREATE GRAPH SILENT X' statement to ensure that the graph exists in the DB. The versions
     * affected were not documented, but version 7.1.0 seems to be ok as long as standard SPARQL 1.1
     * syntax is used.
     * 
     * Should not be an issue in this scenario, since the target graph is the version graph (which
     * already exists & contains information)
     */
    VirtuosoUpdateRequest request = new VirtuosoUpdateRequest(update, virtGraph);
    try {
      request.exec();
    } finally {
      /**
       * NOTE: the VirtGraph opens a connection, which will remain open. To avoid that, it is
       * created & closed in the scope of this update execution
       */
      virtGraph.close();
    }
  }

  @Override
  public QueryExecution queryExecution(String query) {
    VirtGraph virtGraph = new VirtGraph(this.dataSource);
    virtGraph.setReadFromAllGraphs(true);
    return new VirtGraphClosingQueryExecution(
        VirtuosoQueryExecutionFactory.create(query, virtGraph), virtGraph);
  }


  /**
   * Wrapper class for {@link QueryExecution}, overrides the {@link QueryExecution#close()} method
   * to, additionally, close the configured {@link VirtGraph} class, forwards all other methods.
   * 
   * On creation, a VirtGraph gets a connection from the dataSource and uses it for its lifetime
   * (until closed). Therefore, to avoid having "dead connections", the virtGraph object's lifetime
   * is limited to the query lifetime
   * 
   * @author Ruben Navarro Piris
   *
   */
  public static final class VirtGraphClosingQueryExecution extends ForwardingQueryExecution
      implements QueryExecution, Closeable {
    private final VirtGraph virtGraph;

    public VirtGraphClosingQueryExecution(QueryExecution queryExecution, VirtGraph virtGraph) {
      super(queryExecution);
      this.virtGraph = virtGraph;
    }

    /** Extended functionality **/

    @Override
    public void close() {
      try {
        this.virtGraph.close();
      } finally {
        super.close();
      }
    }

  }
}
