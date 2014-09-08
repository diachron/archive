package eu.fp7.diachron.store;


import java.io.IOException;

import virtuoso.jdbc4.VirtuosoDataSource;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

import com.hp.hpl.jena.query.QueryExecution;

/**
 * PROTOTYPE (not tested)
 * 
 * Implementation of {@link SparqlStore} which uses an OpenLink Virtuoso server as data backend.
 *
 * @author Ruben Navarro Piris
 *
 */
public class VirtuosoSparqlStore extends BufferedWriterSparqlStore implements SparqlStore {

  private final VirtuosoDataSource dataSource;

  public VirtuosoSparqlStore(VirtuosoDataSource dataSource, int flushSize) {
    super(flushSize);
    this.dataSource = dataSource;
  }

  // @Override
  // public void storeTriples(String targetGraph, Model model) throws IOException {
  // Model remoteModel = new VirtModel(new VirtGraph(targetGraph, this.dataSource));
  // try {
  // remoteModel.add(model);
  // } finally {
  // remoteModel.close();
  // }
  // }

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

}
