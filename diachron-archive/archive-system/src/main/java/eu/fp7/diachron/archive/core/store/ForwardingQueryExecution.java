package eu.fp7.diachron.archive.core.store;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.util.FileManager;

/**
 * Forwarding implementation for {@link QueryExecution}. Forwards all methods to the configured
 * delegate {@link QueryExecution}. Intended for inheritance.
 * 
 * @author Ruben Navarro Piris
 *
 */
public class ForwardingQueryExecution implements QueryExecution, Closeable {
  private final QueryExecution delegate;

  public ForwardingQueryExecution(QueryExecution queryExecution) {
    this.delegate = queryExecution;
  }

  @Override
  public void close() {
    this.delegate.close();
  }

  @Override
  @Deprecated
  public void setFileManager(FileManager fm) {
    this.delegate.setFileManager(fm);
  }

  @Override
  public void setInitialBinding(QuerySolution binding) {
    this.delegate.setInitialBinding(binding);
  }

  @Override
  public Dataset getDataset() {
    return this.delegate.getDataset();
  }

  @Override
  public Context getContext() {
    return this.delegate.getContext();
  }

  @Override
  public Query getQuery() {
    return this.delegate.getQuery();
  }

  @Override
  public ResultSet execSelect() {
    return this.delegate.execSelect();
  }

  @Override
  public Model execConstruct() {
    return this.delegate.execConstruct();
  }

  @Override
  public Model execConstruct(Model model) {
    return this.delegate.execConstruct(model);
  }

  @Override
  public Iterator<Triple> execConstructTriples() {
    return this.delegate.execConstructTriples();
  }

  @Override
  public Model execDescribe() {
    return this.delegate.execDescribe();
  }

  @Override
  public Model execDescribe(Model model) {
    return this.delegate.execDescribe(model);
  }

  @Override
  public Iterator<Triple> execDescribeTriples() {
    return this.delegate.execDescribeTriples();
  }

  @Override
  public boolean execAsk() {
    return this.delegate.execAsk();
  }

  @Override
  public void abort() {
    this.delegate.abort();
  }

  @Override
  public void setTimeout(long timeout, TimeUnit timeoutUnits) {
    this.delegate.setTimeout(timeout, timeoutUnits);
  }

  @Override
  public void setTimeout(long timeout) {
    this.delegate.setTimeout(timeout);
  }

  @Override
  public void setTimeout(long timeout1, TimeUnit timeUnit1, long timeout2, TimeUnit timeUnit2) {
    this.delegate.setTimeout(timeout1, timeUnit1, timeout2, timeUnit2);
  }

  @Override
  public void setTimeout(long timeout1, long timeout2) {
    this.delegate.setTimeout(timeout1, timeout2);
  }

  @Override
  public long getTimeout1() {
    return this.delegate.getTimeout1();
  }

  @Override
  public long getTimeout2() {
    return this.delegate.getTimeout2();
  }

}
