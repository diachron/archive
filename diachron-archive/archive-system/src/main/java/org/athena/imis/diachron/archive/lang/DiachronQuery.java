package org.athena.imis.diachron.archive.lang;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.athena.imis.diachron.archive.core.dataloader.DictionaryCache;
import org.athena.imis.diachron.archive.core.dataloader.DictionaryService;
import org.athena.imis.diachron.archive.core.dataloader.RDFDictionary;
import org.athena.imis.diachron.archive.core.dataloader.Reconstruct;
import org.athena.imis.diachron.archive.core.dataloader.StoreFactory;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.utils.DiachronQueryUtils;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_OneOf;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.lang.SPARQLParser;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementMinus;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;
import com.hp.hpl.jena.sparql.util.ExprUtils;


public class DiachronQuery extends Query {
	
    //Diachron Elements
  
    private Map<String, HashMap<String, String>> diachronDatasetURIs = new HashMap<String, HashMap<String, String>>();
    private Map<String, HashMap<String, String>> diachronChangesetURIs = new HashMap<String, HashMap<String, String>>();
    private Map<Node, HashMap<String, Node>> diachronDatasetInPatternURIs = new HashMap<Node, HashMap<String, Node>>();
    private Map<Node, HashMap<String, Node>> diachronChangesetInPatternURIs = new HashMap<Node, HashMap<String, Node>>();
    private Map<String, HashMap<String, String>> diachronChangesInPatternURIs = new HashMap<String, HashMap<String, String>>();
    private boolean isFloatingQuery = true;
    public Map<TriplePath, Node> attributeMap = new HashMap<TriplePath, Node>();
    public void addAttribute(TriplePath tp, Node n){ attributeMap.put(tp, n);}
    protected boolean isDiachronQuery = false;
    protected boolean isSchemaDisabled = false;
    protected boolean isSetDiachronSource = false;
    public void setDiachronQuery()               { isDiachronQuery = true ; }
    public boolean isDiachronQuery()               { return isDiachronQuery ; }
    public void setSchemaDisabled()               { isSchemaDisabled = true ; }
    public boolean getSchemaDisabled()               { return isSchemaDisabled ; }
    public boolean getSetDiachronSource()	{ return isSetDiachronSource; }
    public void setSetDiachronSource()	{ isSetDiachronSource = true; }
    public Map<String, HashMap<String, String>> getDiachronDatasetURIs(){ return diachronDatasetURIs ; }
    public Map<String, HashMap<String, String>> getDiachronChangesetURIs(){ return diachronChangesetURIs ; }
    public Map<Node, HashMap<String, Node>> getDiachronDatasetInPatternURIs(){ return diachronDatasetInPatternURIs ; }
    public Map<String, HashMap<String, String>> diachronChangesInPatternURIs(){ return diachronChangesInPatternURIs ; }
    private ElementNamedGraph diachronDictionaryElements;
    public ElementNamedGraph getDiachronDictionaryElements() { return diachronDictionaryElements ; }
    private ElementGroup diachronQueryBody;
    public ElementGroup getDiachronQueryBody() { return diachronQueryBody; }
    public void setDiachronDictionaryElements(ElementNamedGraph elg) { diachronDictionaryElements = elg ; }
    public void setDiachronQueryBody(ElementGroup dqb) { diachronQueryBody = dqb ; }
    public HashSet<String> tempGraphs = new HashSet<String>();
    //private String diachronDictionaryURI = "http://example.com/dictionary";
    public String getDiachronDictionaryURI() { return RDFDictionary.getDictionaryNamedGraph(); }
    /*private int varCounter = 0;
    public String DiachronQueryUtils.getNextVariable(){ return "var"+(varCounter++);}*/
    private static DictionaryService dict = StoreFactory.createDictionaryService();
    
//    private List<String> namedGraphURIs = new ArrayList<>() ;    


    
    
    //Diachron methods here
    /** 
	* Add a diachronic dataset to the sources, taken from the FROM DATASET clause. No specification of version.
     */
    public void addDiachronicDataset(Node diachronicDatasetURI)
    {
    	//Sanity check
    	if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
    	
        if ( diachronDatasetURIs == null )
        	diachronDatasetURIs = new HashMap<String, HashMap<String, String>>(); 
        HashMap<String, String> datasetParams = diachronDatasetURIs.get(diachronicDatasetURI.toString());
    	if (datasetParams == null || datasetParams.isEmpty()) 
    		diachronDatasetURIs.put(diachronicDatasetURI.toString(), null);
    	//else diachronDatasetURIs.put(diachronicDatasetURI.toString(), datasetParams);
    	/*System.out.println("The Diachronic Hash Table contains: ") ;
    	for(String iri : diachronDatasetURIs.keySet()){
    		System.out.println(iri+" := " + diachronDatasetURIs.get(iri));
    	}*/
    	
    }
    
    public void addDiachronicDataset(Node diachronicDatasetURI, ArrayList<Node> datasetVersions, String type)
    {
    	
    	//Sanity check
    	if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
        if ( diachronDatasetURIs == null || diachronDatasetURIs.isEmpty() )
        	diachronDatasetURIs = new HashMap<String, HashMap<String, String>>();
        HashMap<String, String> datasetParams = diachronDatasetURIs.get(diachronicDatasetURI.toString());
    	if (datasetParams == null) datasetParams = new HashMap<String, String>();
        if(type.equals("at")){
        	datasetParams.put("at", datasetVersions.get(0).toString());
        }        
        else if (type.equals("after")){
        	datasetParams.put("after", datasetVersions.get(0).toString());
        }
        else if (type.equals("before")){
        	datasetParams.put("before", datasetVersions.get(0).toString());
        }
        else if (type.equals("between")){
        	datasetParams.put("after", datasetVersions.get(0).toString());
        	datasetParams.put("before", datasetVersions.get(1).toString());
        }
        diachronDatasetURIs.put(diachronicDatasetURI.toString(), datasetParams);
        /*System.out.println("Dataset Params: ") ;
        for(String iri : diachronDatasetURIs.keySet()){
    		System.out.println(iri+" => " + diachronDatasetURIs.get(iri));
    	}*/
    }
    
    public void addDiachronicDatasetInPattern(Node diachronicDataset, ArrayList<Node> datasetVersions, String type)
    {
    	
    	//Sanity check
    	if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
    	
    	
        if ( diachronDatasetInPatternURIs == null || diachronDatasetInPatternURIs.isEmpty() )
        	diachronDatasetInPatternURIs = new HashMap<Node, HashMap<String, Node>>();
        
        HashMap<String, Node> datasetParams = diachronDatasetInPatternURIs.get(diachronicDataset);
    	if (datasetParams == null) datasetParams = new HashMap<String, Node>();
        if(type.equals("at")){
        	datasetParams.put("at", datasetVersions.get(0));
        }        
        else if (type.equals("after")){
        	datasetParams.put("after", datasetVersions.get(0));
        }
        else if (type.equals("before")){
        	datasetParams.put("before", datasetVersions.get(0));
        }
        else if (type.equals("between")){
        	datasetParams.put("after", datasetVersions.get(0));
        	datasetParams.put("before", datasetVersions.get(1));
        }
        diachronDatasetInPatternURIs.put(diachronicDataset, datasetParams);
        /*System.out.println("Dataset Params: ") ;
        for(String iri : diachronDatasetURIs.keySet()){
    		System.out.println(iri+" => " + diachronDatasetURIs.get(iri));
    	}*/
    }
    
    public void addDiachronicChangesetInPattern(Node diachronicDataset, ArrayList<Node> datasetVersions, String type)
    {
    	
    	//Sanity check
    	if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
    	//Node is already cleaned
    	//diachronicDataset = DiachronQueryUtils.getCleanNode(diachronicDataset);
        if ( diachronChangesetInPatternURIs == null || diachronChangesetInPatternURIs.isEmpty() )
        	diachronChangesetInPatternURIs = new HashMap<Node, HashMap<String, Node>>();
        HashMap<String, Node> datasetParams = diachronChangesetInPatternURIs.get(diachronicDataset);
    	if (datasetParams == null) datasetParams = new HashMap<String, Node>();
       if (type.equals("after")){
        	datasetParams.put("after", datasetVersions.get(0));
        }
        else if (type.equals("before")){
        	datasetParams.put("before", datasetVersions.get(0));
        }
        else if (type.equals("between")){
        	datasetParams.put("after", datasetVersions.get(0));
        	datasetParams.put("before", datasetVersions.get(1));
        }
       diachronChangesetInPatternURIs.put(diachronicDataset, datasetParams);
        /*System.out.println("Dataset Params: ") ;
        for(String iri : diachronDatasetURIs.keySet()){
    		System.out.println(iri+" => " + diachronDatasetURIs.get(iri));
    	}*/
    }
    
    
    public void addDiachronicChangesInPattern(String diachronicDatasetURI, ArrayList<String> datasetVersions, String type)
    {
    	
    	//Sanity check
    	if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
    	
        if ( diachronChangesInPatternURIs == null || diachronChangesInPatternURIs.isEmpty() )
        	diachronChangesInPatternURIs = new HashMap<String, HashMap<String, String>>();
        HashMap<String, String> datasetParams = diachronChangesInPatternURIs.get(diachronicDatasetURI.toString());
    	if (datasetParams == null) datasetParams = new HashMap<String, String>();
        if(type.equals("at")){
        	datasetParams.put("at", datasetVersions.get(0).toString());
        }        
        else if (type.equals("after")){
        	datasetParams.put("after", datasetVersions.get(0).toString());
        }
        else if (type.equals("before")){
        	datasetParams.put("before", datasetVersions.get(0).toString());
        }
        else if (type.equals("between")){
        	datasetParams.put("after", datasetVersions.get(0).toString());
        	datasetParams.put("before", datasetVersions.get(1).toString());
        }
        diachronChangesInPatternURIs.put(diachronicDatasetURI.toString(), datasetParams);
        /*System.out.println("Dataset Params: ") ;
        for(String iri : diachronDatasetURIs.keySet()){
    		System.out.println(iri+" => " + diachronDatasetURIs.get(iri));
    	}*/
    }
    
    /** 
   	* Add a diachronic dataset to the sources, taken from the FROM DATASET clause. No specification of version.
        */
       public void addDiachronicChangeset(Node diachronChangesetURI)
       {
       	//Sanity check
       	if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
       	
           if ( diachronChangesetURIs == null )
        	   diachronChangesetURIs = new HashMap<String, HashMap<String, String>>(); 
           HashMap<String, String> datasetParams = diachronChangesetURIs.get(diachronChangesetURI.toString());
       	if (datasetParams == null || datasetParams.isEmpty()) 
       		diachronChangesetURIs.put(diachronChangesetURI.toString(), null);
       	//else diachronDatasetURIs.put(diachronicDatasetURI.toString(), datasetParams);
       	System.out.println("The ChangeSet Hash Table contains: ") ;
       	for(String iri : diachronChangesetURIs.keySet()){
       		System.out.println(iri+" := " + diachronChangesetURIs.get(iri));
       	}
       	
       }
       
       public void addDiachronicChangeset(Node diachronChangesetURI, ArrayList<Node> datasetVersions, String type)
       {
       	
       	//Sanity check
       	if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
           if ( diachronChangesetURIs == null || diachronChangesetURIs.isEmpty() )
        	   diachronChangesetURIs = new HashMap<String, HashMap<String, String>>();
           HashMap<String, String> datasetParams = diachronChangesetURIs.get(diachronChangesetURI.toString());
       	if (datasetParams == null) datasetParams = new HashMap<String, String>();
           if(type.equals("at")){
           	datasetParams.put("at", datasetVersions.get(0).toString());
           }        
           else if (type.equals("after")){
           	datasetParams.put("after", datasetVersions.get(0).toString());
           }
           else if (type.equals("before")){
           	datasetParams.put("before", datasetVersions.get(0).toString());
           }
           else if (type.equals("between")){
           	datasetParams.put("after", datasetVersions.get(0).toString());
           	datasetParams.put("before", datasetVersions.get(1).toString());
           }
           diachronChangesetURIs.put(diachronChangesetURI.toString(), datasetParams);
           System.out.println("Changeset Params: ") ;
           for(String iri : diachronChangesetURIs.keySet()){
       		System.out.println(iri+" => " + diachronChangesetURIs.get(iri));
       	}
       }
              
       

       public void finalizeDiachronDictionaryQuery(){
    	   
    	   
    	   if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;   		
   		  	
   		  	ArrayList<ElementGroup> unionList = new ArrayList<ElementGroup>();
   		  	
   	       	for(String iri : getDiachronDatasetURIs().keySet()){

   	       			DictionaryService dictService = StoreFactory.createDictionaryService();   	       			   	       		
   	       			
   	       			ElementGroup elg_1 = new ElementGroup();	
   		  			HashMap<String, String> datasetParams = getDiachronDatasetURIs().get(iri);
   		  		    ElementTriplesBlock triples = new ElementTriplesBlock();
   		  		    ElementTriplesBlock triplesOpt = new ElementTriplesBlock();   		  		    
   					if(datasetParams != null && datasetParams.containsKey("at")) //First priority
   					{
   					  	String atVersionURI = datasetParams.get("at");
   						Triple t1 = new Triple(NodeFactory.createURI(iri), 
   			 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
   			 			   NodeFactory.createURI(atVersionURI));
   			 			Triple t2 = new Triple(NodeFactory.createURI(atVersionURI), 
   					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasRecordSet"), 
   					 			   NodeFactory.createVariable("rs"));
   					 	if(!getSchemaDisabled())
   					 	{
   						   Triple t3 = new Triple(NodeFactory.createURI(atVersionURI),
   						   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasSchemaSet"),
   						   NodeFactory.createVariable("ss"));
   						   triplesOpt.addTriple(t3);
   					 	}
   					 	 
   					    triples.addTriple(t1);
   					    triples.addTriple(t2);
   					    elg_1.addElement(triples);
   						if(!triplesOpt.isEmpty()) elg_1.addElement(new ElementOptional(triplesOpt));
   					}
   					else
   					{   					  	  
   					  	  
   					  	        String nextVar = DiachronQueryUtils.getNextVariable();					  	        			  	          														
   								Triple t1 = new Triple(NodeFactory.createURI(iri), 
   					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
   					 			   NodeFactory.createVariable(nextVar));
   					 			Triple t2 = new Triple(NodeFactory.createVariable(nextVar), 
   							 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasRecordSet"), 
   							 			   NodeFactory.createVariable("rs"));
   					 			
   					 			String dateVar1 = DiachronQueryUtils.getNextVariable();	
   								Triple t2date1 = new Triple(NodeFactory.createVariable(nextVar), 
   							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
   							 			   NodeFactory.createVariable(dateVar1));   							 						   							 			   
   								ExprList l1 = new ExprList();
   						 	    l1.add(new ExprVar(dateVar1));
   						 	    triples.addTriple(t1);
					    		triples.addTriple(t2);
					    		if(datasetParams != null)
					    			triples.addTriple(t2date1);
   						 	    if(!getSchemaDisabled())
   							 	{
   								   Triple t3 = new Triple(NodeFactory.createVariable(nextVar), 
   								   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasSchemaSet"), 
   								   NodeFactory.createVariable("ss"));
   								   triplesOpt.addTriple(t3);
   							 	}
   							 						    		
   					    		if(!triplesOpt.isEmpty()) elg_1.addElement(new ElementOptional(triplesOpt));	
   					    if(datasetParams != null && (datasetParams.containsKey("after") || datasetParams.containsKey("before"))){
   						  if(datasetParams.containsKey("after"))
   							{
   								String afterVersionURI = datasetParams.get("after");
   								String dateVar2 = DiachronQueryUtils.getNextVariable();
   								Triple t2date2 = new Triple(NodeFactory.createURI(afterVersionURI), 
   							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
   							 			   NodeFactory.createVariable(dateVar2));
   							 	triples.addTriple(t2date2);
   							 	ExprList l2 = new ExprList();
   						 	    l2.add(new ExprVar(dateVar2));
   						 	      
   							 	ElementFilter afterFilter = new ElementFilter(new E_GreaterThanOrEqual(new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l1), new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l2)));
   							 	elg_1.addElement(afterFilter);		   							 								   					    		
   								
   							}
   						  if(datasetParams.containsKey("before"))
   							{
   							  	String beforeVersionURI = datasetParams.get("before");
   							  	String dateVar2 = DiachronQueryUtils.getNextVariable();
   							  	Triple t2date2 = new Triple(NodeFactory.createURI(beforeVersionURI), 
   							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
   							 			   NodeFactory.createVariable(dateVar2));
   							 	triples.addTriple(t2date2);
   							 	ExprList l2 = new ExprList();
   						 	    l2.add(new ExprVar(dateVar2));												 	    
   							 	ElementFilter beforeFilter = new ElementFilter(new E_LessThanOrEqual(new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l1), new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l2)));
   							 	elg_1.addElement(beforeFilter);		   							 	
   							}   							
   					  	  }
   					    elg_1.addElement(triples);   					  	 
   					  	  
   					}
   				unionList.add(elg_1);											  		
   		  			
   	       	}	       	
   	       	if(unionList.size() > 1)
   	       	{
   	       	  
   				ElementUnion elu = new ElementUnion();
   				for(ElementGroup elg : unionList)
   				{
   					elu.addElement(elg);
   				}
   				setDiachronDictionaryElements(new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), elu));
   			}
   			else if(unionList.size()==1)
   			{			  
   				ElementGroup elu = unionList.get(0);
   				setDiachronDictionaryElements(new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), elu));
   			}
   			
   		}
       
       public void finalizeDiachronChangesetDictionaryQuery(){
    	   if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;   		
   		  	
   		  	ArrayList<ElementGroup> unionList = new ArrayList<ElementGroup>();   		 
   	       	for(String iri : getDiachronChangesetURIs().keySet()){
   	       			
   	       			ElementGroup elg_1 = new ElementGroup();	
   		  			HashMap<String, String> datasetParams = getDiachronChangesetURIs().get(iri);
   		  		    ElementTriplesBlock triples = new ElementTriplesBlock();
   		  		     
   		  		    String nextVar = DiachronQueryUtils.getNextVariable();			
   		  		    Triple t1 = new Triple(NodeFactory.createURI(iri), 
			 			  /* NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
			 			   NodeFactory.createVariable(nextVar));
			 		Triple t2 = new Triple(NodeFactory.createVariable(nextVar), */
					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasChangeSet"), 
					 			   NodeFactory.createVariable("cs"));
			 		triples.addTriple(t1);
			    	//triples.addTriple(t2);
   					if(datasetParams != null && (datasetParams.containsKey("after") || datasetParams.containsKey("before")))
   					  	  {
   					  	      		  	        			  	       
   								String dateVar1 = DiachronQueryUtils.getNextVariable();								
   								
   								Triple t2date1 = new Triple(NodeFactory.createVariable(nextVar), 
   							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
   							 			   NodeFactory.createVariable(dateVar1));
   							 						   							 			   
   								ExprList l1 = new ExprList();
   						 	    l1.add(new ExprVar(dateVar1));   						 	      						 	      							 	
   					    		triples.addTriple(t2date1);					    		   					    					    	
   						  if(datasetParams.containsKey("after"))
   							{
   								String afterVersionURI = datasetParams.get("after");
   								String dateVar2 = DiachronQueryUtils.getNextVariable();
   								Triple t2date2 = new Triple(NodeFactory.createURI(afterVersionURI), 
   							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
   							 			   NodeFactory.createVariable(dateVar2));
   							 	triples.addTriple(t2date2);
   							 	ExprList l2 = new ExprList();
   						 	    l2.add(new ExprVar(dateVar2));
   						 	      
   							 	ElementFilter afterFilter = new ElementFilter(new E_GreaterThanOrEqual(new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l1), new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l2)));
   							 	elg_1.addElement(afterFilter);		   							 								   					    		
   								
   							}
   						  if(datasetParams.containsKey("before"))
   							{
   							  	String beforeVersionURI = datasetParams.get("before");
   							  	String dateVar2 = DiachronQueryUtils.getNextVariable();
   							  	Triple t2date2 = new Triple(NodeFactory.createURI(beforeVersionURI), 
   							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
   							 			   NodeFactory.createVariable(dateVar2));
   							 	triples.addTriple(t2date2);
   							 	ExprList l2 = new ExprList();
   						 	    l2.add(new ExprVar(dateVar2));												 	    
   							 	ElementFilter beforeFilter = new ElementFilter(new E_LessThanOrEqual(new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l1), new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l2)));
   							 	elg_1.addElement(beforeFilter);		   							 	
   							}
   							
   					  	  }
   					elg_1.addElement(triples);
   					  	  
   					
   				unionList.add(elg_1);											  		
   		  			
   	       	}	       	
   	       	if(unionList.size() > 1)
   	       	{
   	       	  
   				ElementUnion elu = new ElementUnion();
   				for(ElementGroup elg : unionList)
   				{
   					elu.addElement(elg);
   				}
   				ElementNamedGraph all = getDiachronDictionaryElements();
   				ElementGroup g = new ElementGroup();
   				g.addElement(all.getElement());
   				g.addElement(elu);
   				setDiachronDictionaryElements(new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), g));
   			}
   			else if(unionList.size()==1)
   			{			  
   				ElementGroup elu = unionList.get(0);
   				ElementNamedGraph all = getDiachronDictionaryElements();
   				ElementGroup g = new ElementGroup();
   				g.addElement(elu);
   				if(all != null){   					
   	   				g.addElement(all.getElement());   	   				
   				}
   				
   				setDiachronDictionaryElements(new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), g));
   			}
   			
   		}
       
       public Element createDiachronDatasetInPatternElement(Node diachronicDataset, Element pattern){
    	   
    	   if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
    	   
    	   isFloatingQuery = false;
    	   
    	   ElementUnion diachronicUnion = new ElementUnion();
    	   
    	   List<Node> diachronicList = DiachronQueryUtils.getListOfDiachronicDatasets(diachronicDataset);
    	   
    	   for(Node nextDiachronicNode : diachronicList){
    		   
    		   if(dict.getDiachronicDataset(nextDiachronicNode.getURI()) == null)
        	   {
        		   throw new QueryException("Diachronic dataset does not exist. ") ;
        	   }
        	   
        	   if(!getDiachronDatasetURIs().isEmpty() && !getDiachronDatasetURIs().containsKey(nextDiachronicNode.getURI()))
    			  	throw new QueryException("Diachronic dataset in DATASET clause is not specified in FROM clause.");      	   
        	   
        	   ElementGroup inputPattern = (ElementGroup) pattern;    	      	       	   
        	           	   
        	   ElementGroup fakeMatGroup = new ElementGroup();
        	   
        	   HashMap<String, Node> datasetParams = DiachronQueryUtils.getDatasetParameters(diachronDatasetInPatternURIs.get(nextDiachronicNode), diachronDatasetInPatternURIs.get(diachronicDataset));
        	   
      		   if(datasetParams != null && datasetParams.containsKey("at")) //First priority
      					{
      					  	Node atVersionURI = datasetParams.get("at");
      					  	
      					  	atVersionURI = DiachronQueryUtils.getCleanNode(atVersionURI);
      					  	
      					  	if(atVersionURI.isURI()){
      					  		
    	  					  	 if(dict.getDataset(atVersionURI.getURI())==null)	{	  			    	   		  			    	   
    		  			    		   throw new QueryException("Dataset version does not exist. ") ;
    		  			    	   }
      					  		
      					  		if(!getDiachronDatasetURIs().isEmpty() && !getDiachronDatasetURIs().get(nextDiachronicNode.getURI()).get("at").equals(atVersionURI.getURI()))
      					  			throw new QueryException("Diachronic dataset in DATASET clause is not specified in FROM clause.");  
      					  		
      					  		if(!dict.getDataset(atVersionURI.getURI()).isFullyMaterialized()){
      					  			try {
      					  				
      					  				List<Dataset> list = new ArrayList<Dataset>();

      					  				list.add(dict.getDataset(atVersionURI.getURI()));

      					  				//fakeMatGroup = materializeDataset(list, dict.getDiachronicDataset(diachronicDataset.getURI().toString()), fakeMatGroup, inputPattern);
      					  				HashMap<Dataset, String[]> matMap = fakeMaterializeDataset(list, dict.getDiachronicDataset(diachronicDataset.getURI().toString()));

      					  				fakeMatGroup = addFakeQueryElements(fakeMatGroup, matMap, inputPattern);

      					  			} 
      					  			catch(Exception e){
      					  			// TODO Auto-generated catch block
    									e.printStackTrace();
      					  			}
      					  		}
      					  		else{
      					  			
      					  			String rsURI = dict.getDataset(atVersionURI.getURI()).getRecordSet().getId().toString();
      					  			
      					  			fakeMatGroup.addElement(new ElementNamedGraph(NodeFactory.createURI(rsURI), pattern));
      					  			
      					  		}
      					  			
      					  	}
      					  	else if(atVersionURI.isVariable()){
      					  		
      					  		ElementUnion variableSourceUnion = new ElementUnion();
      					  		///Source selection
      					  		try{
    	  					  		
      					  			List<Dataset> list = variableSourceSelection(inputPattern, nextDiachronicNode.getURI(), null, null);
    	  					  		
      					  			/*System.out.println(list.size());
      					  			System.out.println(inputPattern.toString());
      					  			System.out.println(nextDiachronicNode.getURI());*/

    	  					  		List<Dataset> nonMaterializedList ;

    	  					  		ExprList inlist = new ExprList();

    		  					  	for(Dataset dataset : list){

    			  					  	if(!getDiachronDatasetURIs().isEmpty() && !getDiachronDatasetURIs().get(nextDiachronicNode.getURI()).get("at").equals(dataset.getId()))
    		  					  			continue;


    	  					  			if(!dataset.isFullyMaterialized()){

    	  					  				nonMaterializedList = new ArrayList<>();

    	  					  				nonMaterializedList.add(dataset);

    	  					  				HashMap<Dataset, String[]> varFakeMap = fakeMaterializeDataset(nonMaterializedList, dict.getDiachronicDataset(nextDiachronicNode.getURI().toString()));

    	  					  				ElementGroup fakeMats = new ElementGroup();

    	  					  				fakeMats = addFakeQueryElements(fakeMats, varFakeMap, inputPattern, atVersionURI.getName().toString(), diachronicDataset);

    	  					  				if(!fakeMats.isEmpty())
    	  					  					variableSourceUnion.addElement(fakeMats);

    	  					  			}
    	  					  			else{

    		  					  			inlist.add(ExprUtils.nodeToExpr(NodeFactory.createURI(dataset.getRecordSet().getId().toString())));			  					  			  					  					  					  								  			

    	  					  			}

    	  					  		}

    		  					  if(!inlist.isEmpty()){

    		  						  String var_full_mat_uri = DiachronQueryUtils.getNextVariable();

    			  					  Expr e = new E_OneOf(new ExprVar(var_full_mat_uri), inlist);

    			  					  ElementFilter filter = new ElementFilter(e);

    			  					  Triple dictionaryTriple = new Triple(atVersionURI, NodeFactory.createURI(DiachronOntology.hasRecordSet.getURI()), NodeFactory.createVariable(var_full_mat_uri)); 	

    			  					  ElementGroup fullDatasets = new ElementGroup();

    			  					  ElementTriplesBlock dictBlock = new ElementTriplesBlock();

    			  					  dictBlock.addTriple(dictionaryTriple);

    			  					  if(diachronicDataset.isVariable()){

    			  						Triple dictionaryTriple2 = new Triple(diachronicDataset, NodeFactory.createURI(DiachronOntology.hasInstantiation.getURI()), atVersionURI);

    			  						dictBlock.addTriple(dictionaryTriple2);

    			  					  }

    			  					  fullDatasets.addElement(new ElementNamedGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), dictBlock));

    			  					  fullDatasets.addElement(filter);
    			  					  
    			  					  fullDatasets.addElement(new ElementNamedGraph(NodeFactory.createVariable(var_full_mat_uri), inputPattern));
    			  					  
    			  					  variableSourceUnion.addElement(fullDatasets);
    			  					      			  					     		  						  
    		  					  }
    		  					      		  					      		  					  
    		  					  if(variableSourceUnion.getElements().size() > 1) { 
    		  						    		  						  
    		  						  fakeMatGroup.addElement(variableSourceUnion);
    		  						  
    		  					  }
    		  						  
    		  					  else if (variableSourceUnion.getElements().size() == 1){
    		  						  
    		  						  fakeMatGroup.addElement(variableSourceUnion.getElements().get(0));
    		  						  
    		  					  }
    		  					  
    		  					  else continue;
    		  					  
    		  					  ExprList all_datasets = new ExprList();
    		  					  
    		  					  for(Dataset dataset : list){
    		  						  
    		  						all_datasets.add(ExprUtils.nodeToExpr(NodeFactory.createURI(dataset.getId().toString())));
    		  						  
    		  					  }
    		  					  	
    		  					  
    		  					  Expr e = new E_OneOf(new ExprVar(atVersionURI.getName().toString()), all_datasets);
    	  					  	  
    		  					  ElementFilter filter = new ElementFilter(e);
    		  				
    		  					  fakeMatGroup.addElement(filter);		  					 		  					
    		  					   
      					  		}
      					  		catch(Exception e){ e.printStackTrace(); if(true) return null;}
      					  		      		  				
      					  	}  										   
      					}
      					else
      					{
      					  	  
      					  	       
      					    if(datasetParams != null && (datasetParams.containsKey("after") || datasetParams.containsKey("before"))){
      						        							    							  
      							  try{
      								  
      								Node afterVersionURI = null;
      								
      								if(datasetParams.containsKey("after")) {
      									  									
      									afterVersionURI = datasetParams.get("after");
      									
      									afterVersionURI = DiachronQueryUtils.getCleanNode(afterVersionURI );
      									
      									if(dict.getDataset(afterVersionURI.getURI())==null)	{	  			    	   		  			    	   
      			  			    		   throw new QueryException("Dataset version does not exist. ") ;
      			  			    	   }
      								}
      							  	
      								Node beforeVersionURI = null;
      								
      								if(datasetParams.containsKey("before")) {
      									  									
      									beforeVersionURI = datasetParams.get("before");
      									
      									beforeVersionURI = DiachronQueryUtils.getCleanNode(beforeVersionURI );
      									
      									if(dict.getDataset(beforeVersionURI.getURI())==null)	{	  			    	   		  			    	   
       			  			    		   throw new QueryException("Dataset version does not exist. ") ;
       			  			    	   }
      								}
      								
    								
    									List<Dataset> list = variableSourceSelection(inputPattern, nextDiachronicNode.getURI(), afterVersionURI, beforeVersionURI);
    									
    									List<Dataset> nonMaterializedList ;
    									
    									ElementUnion variableSourceUnion = new ElementUnion();
    		  					  		
    									ExprList inlist = new ExprList();
    		  					  		
    									Node variableDataset = NodeFactory.createVariable(DiachronQueryUtils.getNextVariable());
    									
    			  					  	for(Dataset dataset : list){
    			  					  		
    				  					  	if(!getDiachronDatasetURIs().isEmpty() && !getDiachronDatasetURIs().get(nextDiachronicNode.getURI()).get("after").equals(dataset.getId()))
    			  					  			continue;
    			  					  		
    			  					  		
    		  					  			if(!dataset.isFullyMaterialized()){
    		  					  					  					  					  				
    		  					  				nonMaterializedList = new ArrayList<>();
    		  					  			
    		  					  				nonMaterializedList.add(dataset);
    		  					  				
    		  					  				HashMap<Dataset, String[]> varFakeMap = fakeMaterializeDataset(nonMaterializedList, dict.getDiachronicDataset(nextDiachronicNode.getURI().toString()));

    		  					  				ElementGroup fakeMats = new ElementGroup();
    			  		  					 
    		  					  				fakeMats = addFakeQueryElements(fakeMats, varFakeMap, inputPattern, variableDataset.getName().toString(), diachronicDataset);
    			  		  					   
    		  					  				variableSourceUnion.addElement(fakeMats);
    			  		  					   	  					  					  					  				
    		  					  			}
    		  					  			else {
    		  					  				
    			  					  			inlist.add(ExprUtils.nodeToExpr(NodeFactory.createURI(dataset.getRecordSet().getId().toString())));			  					  			  					  					  					  								  			
    		  					  			
    		  					  			}
    		  					  				  					  			  					  		
    		  					  		}
    			  							
    				  					  if(!inlist.isEmpty()){
    				  						  
    				  						  String var_full_mat_uri = DiachronQueryUtils.getNextVariable();
    					  					  
    					  					  Expr e = new E_OneOf(new ExprVar(var_full_mat_uri), inlist);
    				  					  	  
    					  					  ElementFilter filter = new ElementFilter(e);
    					  					  
    					  					  Triple dictionaryTriple = new Triple(variableDataset, NodeFactory.createURI(DiachronOntology.hasRecordSet.getURI()), NodeFactory.createVariable(var_full_mat_uri)); 	
    					  					  
    					  					  ElementGroup fullDatasets = new ElementGroup();
    					  					  
    					  					  ElementTriplesBlock dictBlock = new ElementTriplesBlock();
    					  					  
    					  					  dictBlock.addTriple(dictionaryTriple);
    					  					  
    					  					  fullDatasets.addElement(new ElementNamedGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), dictBlock));
    					  					  
    					  					  fullDatasets.addElement(filter);
    					  					  
    					  					  fullDatasets.addElement(new ElementNamedGraph(NodeFactory.createVariable(var_full_mat_uri), inputPattern));
    					  					  
    					  					  variableSourceUnion.addElement(fullDatasets);
    					  					  
    					  					  //fakeMatGroup.addElement(variableSourceUnion);
    				  						  
    				  					  }
    				  					  
    				  					  if(variableSourceUnion.getElements().size() > 1)
    				  						  fakeMatGroup.addElement(variableSourceUnion);
    				  					  else{
    				  						  fakeMatGroup.addElement(variableSourceUnion.getElements().get(0));
    				  					  }
    				  					
    				  					  ExprList all_datasets = new ExprList();
    				  					  
    				  					  for(Dataset dataset : list){
    				  						  
    				  						all_datasets.add(ExprUtils.nodeToExpr(NodeFactory.createURI(dataset.getId().toString())));
    				  						  
    				  					  }
    				  					  	
    				  					  
    				  					  Expr e = new E_OneOf(new ExprVar(variableDataset.getName().toString()), all_datasets);
    			  					  	  
    				  					  ElementFilter filter = new ElementFilter(e);
    				  				
    				  					  fakeMatGroup.addElement(filter);									 
    	  								
    									
    				  					if(afterVersionURI!=null && afterVersionURI.isVariable() && beforeVersionURI!=null && beforeVersionURI.isVariable()){
    										throw new QueryException("Temporal operators (BEFORE, AFTER, BETWEEN) cannot be followed by var.");
    									}
    	  								  
    	  							  }
    	  							  catch(QueryException e){ 
    									  throw e;
    								  }
    	  							  catch(Exception e){ 
    	  								  e.printStackTrace();
    	  							  }
      					  	  }  					      					  	 
      					  	  
      					}
      			if(!fakeMatGroup.isEmpty())
      				diachronicUnion.addElement(fakeMatGroup);	    		   
    	   }
    	   
    	   if(diachronicUnion.getElements().size() > 1)
    		   return diachronicUnion  ;
    	   else return diachronicUnion.getElements().get(0);
    	   
    	   
  		  			  				  	       
       }
       
      
       
       private void bulkLoadRDFDataToGraph(InputStream stream, String graphName) throws Exception {
   		
   		String fileExtension = ".rdf";
   		
   		String fileName = "upload_file."+(new Date()).getTime()+fileExtension;
   		Path path = Paths.get(StoreConnection.getBulkLoadPath()+fileName);
   		Connection conn = null;
   		Statement statement = null;
   		try {
   			Files.copy(stream, path);
   			// do the ISQL stuff
   			conn = StoreConnection.getConnection();		     
   			statement = conn.createStatement();			
   			String deletePastUploads = "delete from db.dba.load_list where ll_state='2'";
   			statement.execute(deletePastUploads);
   		    String bulkLoadSetQuery = "ld_dir('"+StoreConnection.getBulkLoadPath()+"', '"+fileName+"', '"+graphName+"')";		    
   		    statement.execute(bulkLoadSetQuery);
   		    String runBulkLoader = "rdf_loader_run()";
   		    statement.execute(runBulkLoader);
   		    //statement.close();
   			
   		} catch (Exception e) {}
   		finally {		    
   		    try { if (statement != null) statement.close(); } catch (Exception e) {};
   		    try { if (conn != null) conn.close(); } catch (Exception e) {};   		    
   		}
   		
   	}
       
       private List<Dataset> variableSourceSelection(ElementGroup pattern, String diachronicDatasetId, Node after, Node before) throws Exception{
    	   
    	   List<Dataset> list = new ArrayList<>();
    	   
    	   String query;
    	   
    	   QueryExecution qexec;
    	       	       	   
    	   for(Dataset candidateDataset : dict.getListOfDatasets(dict.getDiachronicDataset(diachronicDatasetId))){
    		   
    		   boolean forFull = false;
    		   
    		   ElementGroup fakeElements = new ElementGroup();
    		   if(!candidateDataset.isFullyMaterialized()) {
    			   //TODO
    			   List<Dataset> singleList = new ArrayList<>();
    			   singleList.add(candidateDataset);
    			   HashMap<Dataset, String[]> matMap = fakeMaterializeDataset(singleList, dict.getDiachronicDataset(diachronicDatasetId));
		  				    			   
    			   fakeElements = addFakeQueryElements(fakeElements, matMap, pattern);
    		   }
    		   else{
    			   fakeElements = pattern;
    			   forFull = true;
    		   }
    		   
    		   VirtGraph graph =  (VirtGraph) StoreConnection.getVirtGraph();
   			
    		   graph.setReadFromAllGraphs(true);
    		   
    		   VirtuosoQueryExecution vqe ;
    		       		   
    		   if(after != null){
    			   
	    			  String afterQuery = " ASK { GRAPH <"+RDFDictionary.getDictionaryNamedGraph()+"> {"
	    			  								+ "<"+candidateDataset.getId()+"> <"+DiachronOntology.creationTime+"> ?d1 ."
	    			  								+ "<"+after.getURI()+"> <"+DiachronOntology.creationTime+"> ?d2 ."
	    			  								+ "FILTER (?d1 >= ?d2)"
	    			  								+ "}}";
	    			 
	    			  vqe = VirtuosoQueryExecutionFactory.create(afterQuery, graph);
	       		   
	       		   	  if(!vqe.execAsk()) continue; 
						 	
    		   }
    		   
    		   if(before != null){
    			   
	    			  String beforeQuery = " ASK { GRAPH <"+RDFDictionary.getDictionaryNamedGraph()+"> {"
	    			  								+ "<"+candidateDataset.getId()+"> <"+DiachronOntology.creationTime+"> ?d1 ."
	    			  								+ "<"+before.getURI()+"> <"+DiachronOntology.creationTime+"> ?d2 ."
	    			  								+ "FILTER (?d1 <= ?d2)"
	    			  								+ "}}";
	    			 
	    			  vqe = VirtuosoQueryExecutionFactory.create(beforeQuery, graph);
	       		   
	       		   	  if(!vqe.execAsk()) continue; 
						 	
    		   }
    		   
    		   if(!forFull){
    			   
    			   query = " ASK "    	    		   		
    	    		   				+ " {{SELECT * "    			    		   		
	    			    		   		+ " WHERE " 
	    			    		   		+ fakeElements.toString().replaceAll("\\. \\.", "\\. ")
    			    		   		+" }}";
    			   
    			   
    		   }
    		   
    		   else {
    			
    			   query = " ASK "    		   		   		
    		   		   	+ " {"
    		   		   	+ "		GRAPH <"+candidateDataset.getRecordSet().getId()+">" 
    		   		   	+ 			fakeElements.toString().replaceAll("\\. \\.", "\\. ")
    		   		   	+" }";
    			   
    		   }    			       			    		      		   
    		   
    		   vqe = VirtuosoQueryExecutionFactory.create(query, graph);
    		   
    		   if(vqe.execAsk()) list.add(candidateDataset);
    		   
    		   vqe.close();
    		   
    		   graph.close();
    		   
    	   }    	       	   
    	   
    	   return list;
    	   
       }
       
       public boolean isAfter(Dataset candidateDataset, Node after){
    	   
    	   VirtGraph graph =  (VirtGraph) StoreConnection.getVirtGraph();
  			
		   graph.setReadFromAllGraphs(true);
		   
		   VirtuosoQueryExecution vqe ;
		       		   
		   boolean returnB = false;
		   
		   String afterQuery = " ASK { GRAPH <"+RDFDictionary.getDictionaryNamedGraph()+"> {"
    			  								+ "<"+candidateDataset.getId()+"> <"+DiachronOntology.creationTime+"> ?d1 ."
    			  								+ "<"+after.getURI()+"> <"+DiachronOntology.creationTime+"> ?d2 ."
    			  								+ "FILTER (?d1 >= ?d2)"
    			  								+ "}}";
    			 
		   vqe = VirtuosoQueryExecutionFactory.create(afterQuery, graph);
       		   
       		if(vqe.execAsk()) 
       		   		  returnB = true;
       		   	  					 			 
		   vqe.close();
		   
		   graph.close();
		   
		   return returnB;
		   		  
    	   
       }
       
       public boolean isBefore(Dataset candidateDataset, Node before){
    	   
    	   VirtGraph graph =  (VirtGraph) StoreConnection.getVirtGraph();
  			
		   graph.setReadFromAllGraphs(true);
		   
		   VirtuosoQueryExecution vqe ;
		       		   
		   boolean returnB = false;
		   
		   String beforeQuery = " ASK { GRAPH <"+RDFDictionary.getDictionaryNamedGraph()+"> {"
    			  								+ "<"+candidateDataset.getId()+"> <"+DiachronOntology.creationTime+"> ?d1 ."
    			  								+ "<"+before.getURI()+"> <"+DiachronOntology.creationTime+"> ?d2 ."
    			  								+ "FILTER (?d1 <= ?d2)"
    			  								+ "}}";
    			 
		   vqe = VirtuosoQueryExecutionFactory.create(beforeQuery, graph);
       		   
       		if(vqe.execAsk()) 
       		   		  returnB = true;
       		   	  					 			 
		   vqe.close();
		   
		   graph.close();
		   
		   return returnB;
		   
       }
              
       
       private ElementGroup materializeDataset(List<Dataset> datasetList, DiachronicDataset diachronicDataset, ElementGroup fakeMatGroup, ElementGroup inputPattern) throws Exception{
    	   
    	
    	   for(Dataset dataset : datasetList){
    		      		   
        	  //dict.getDataset(atVersionURI.getURI()); 
    		   String last = dataset.getLastFullyMaterialized();    	   
        	   List<String> changesets = dataset.getListOfChangesets();
        	  /* System.out.println("Last fully materialized version for " + atVersionURI.getURI() + " is " + last);
        	   System.out.println("ChangeSets:");*/
        	   String tempId = ""+System.nanoTime();
        	   //String tempgraph = atVersionURI+"_reconstructed"+tempId;
        	   String tempgraph = dataset.getRecordSet().getId();
        	   Model toAppendAdd = ModelFactory.createDefaultModel();
        	   Model toAppendDelete = ModelFactory.createDefaultModel();
        	   
        	   int count = 0;
        	   for(String cs : changesets){
        		   
        		   System.out.println(cs);
        		   
        		   Reconstruct rec = new Reconstruct(cs, diachronicDataset);
        		   
        		   if(count==0){
        			  
        				   toAppendAdd = rec.getAddedModel();
        								
            			   toAppendDelete = rec.getDeletedModel();
            			   		       			   
        		   }
        		   else{
        			   
        			   toAppendAdd.add(rec.getAddedModel());
        			   
        			   StmtIterator it = toAppendDelete.listStatements();    			   
        			   
        			   while(it.hasNext()){
        				   
        				   toAppendAdd.remove(it.next());
        				   
        			   }
        			   
        			   toAppendDelete.add(rec.getDeletedModel());
        			   
        		   }
        		   count++;
        		   
        	   }    	   
        	   String addedPath = StoreConnection.getBulkLoadPath()+"added.rdf";
        	   String deletedPath = StoreConnection.getBulkLoadPath()+"deleted.rdf";
    	    	   try {
    				toAppendAdd.write(new FileOutputStream(new File(addedPath)));
    			} catch (FileNotFoundException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    	   		
    	   		   try {
    				toAppendDelete.write(new FileOutputStream(new File(deletedPath)));
    			} catch (FileNotFoundException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    	   		   
    	   		FileInputStream input = new FileInputStream(new File(addedPath));
    	   		bulkLoadRDFDataToGraph(input, tempId+"added");
    	   		input = new FileInputStream(new File(deletedPath));
    	   		bulkLoadRDFDataToGraph(input, tempId+"deleted");
    	   		String query = "INSERT { GRAPH <"+tempgraph+"> {" +
    					"?s ?p ?o" +  
    					//"[ diachron:subject ?s ; diachron:hasRecordAttribute [diachron:predicate ?p ; diachron:object ?o]] " + //this is the reified, but uses blank nodes 
    				"} } WHERE { "
    				//+ "GRAPH <"+getDiachronDictionaryURI()+"> {<"+last+"> <"+DiachronOntology.hasRecordSet+"> ?rs }"
    				+ "{ GRAPH <"+dict.getDataset(last).getRecordSet().getId()+"> " 
    						
    						+ " {?s ?p ?o }"
    						+ "MINUS { GRAPH <"+tempId+"deleted> {?s ?p ?o}}} "
    						+ "UNION {GRAPH <"+tempId+"added> {?s ?p ?o}} "
    						+ "}";			
    			VirtGraph virt = StoreConnection.getVirtGraph();
    			VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, virt);
    			vur.exec();
    			
    			query = "DELETE { GRAPH <"+getDiachronDictionaryURI()+"> {" +
    					"<"+dataset.getId()+"> <"+DiachronOntology.isFullyMaterialized+"> ?fm " +
    				"} } WHERE {GRAPH <"+getDiachronDictionaryURI()+"> {" +
    					"<"+dataset.getId()+"> <"+DiachronOntology.isFullyMaterialized+"> ?fm "
    				+ "}} ";
    			virt = StoreConnection.getVirtGraph();
    			vur = VirtuosoUpdateFactory.create(query, virt);
    			vur.exec();
    			
    			query = "INSERT { GRAPH <"+getDiachronDictionaryURI()+"> {" +
    					"<"+dataset.getId()+"> <"+DiachronOntology.isFullyMaterialized+"> \"temp\" " +
    				"} } ";			
    			virt = StoreConnection.getVirtGraph();
    			vur = VirtuosoUpdateFactory.create(query, virt);
    			vur.exec();
    	   		dataset.setFullyMaterialized(true);
    	   		fakeMatGroup.addElement(new ElementNamedGraph(NodeFactory.createURI(dict.getDataset(last).getRecordSet().getId()), inputPattern));
        	   
    	   }
    	   return fakeMatGroup;
		
       }
             
       
       private HashMap<Dataset, String[]> fakeMaterializeDataset(List<Dataset> datasetList, DiachronicDataset diachronicDataset) throws Exception{
    	   
       	
    	   HashMap<Dataset, String[]> returnMap = new HashMap<Dataset, String[]>();
    	   
    	   for(Dataset dataset : datasetList){
    		      		           	       		
    		   if(dataset.getAddedGraphId() != null && dataset.getDeletedGraphId() != null){
    			   
    			   returnMap.put(dataset, new String[]{dataset.getAddedGraphId(), dataset.getDeletedGraphId()});
    			   
    			   continue;
    			   
    		   }
    		   
    		   List<String> changesets = dataset.getListOfChangesets();
        	  
    		   if(changesets == null) continue; 
    		   
        	   String tempId = ""+System.nanoTime();
        	          	           	  
        	   Model toAppendAdd = ModelFactory.createDefaultModel();
        	   
        	   Model toAppendDelete = ModelFactory.createDefaultModel();
        	   
        	   int count = 0;
        	   
        	   for(String cs : changesets){
        		           		           		   
        		   Reconstruct rec = new Reconstruct(cs, diachronicDataset);
        		   
        		   if(count==0){
        			  
        				   toAppendAdd = rec.getAddedModel();
        								
            			   toAppendDelete = rec.getDeletedModel();
            			   		       			   
        		   }
        		   else{
        			   
        			   toAppendAdd.add(rec.getAddedModel());
        			   
        			   StmtIterator it = toAppendDelete.listStatements();    			   
        			   
        			   while(it.hasNext()){
        				   
        				   toAppendAdd.remove(it.next());
        				   
        			   }
        			   
        			   toAppendDelete.add(rec.getDeletedModel());
        			   
        		   }
        		   count++;
        		   
        	   }    	   
        	   
        	   String addedPath = StoreConnection.getBulkLoadPath()+"added.rdf";
        	   
        	   String deletedPath = StoreConnection.getBulkLoadPath()+"deleted.rdf";
        	   
        	   FileOutputStream output;
        	   
    	    	   try {
    	    		   
    	    		output = new FileOutputStream(new File(addedPath));
    			
    	    		toAppendAdd.write(output);
    				
    	    		output.close();
    				
    			} catch (FileNotFoundException e) {
    				
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    	   		
    	   		   try {
    	   			   
    	   			output = new FileOutputStream(new File(deletedPath));
    				toAppendDelete.write(output);
    				output.close();
    				
    			} catch (FileNotFoundException e) {
    				
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    	   		   
    	   		FileInputStream input = new FileInputStream(new File(addedPath));
    	   		
    	   		bulkLoadRDFDataToGraph(input, tempId+"added");
    	   		
    	   		input.close();
    	   		
    	   		input = new FileInputStream(new File(deletedPath));
    	   		
    	   		bulkLoadRDFDataToGraph(input, tempId+"deleted");
    	   		    			
    	   		input.close();
    	   		
    	   		try { for(File file: new File(StoreConnection.getBulkLoadPath()).listFiles()) file.delete();  } catch(Exception e) {e.printStackTrace();};
    	   		
    	   		returnMap.put(dataset, new String[]{tempId+"added", tempId+"deleted"});
    	   		
    	   		tempGraphs.add(tempId+"added");
    	   		
    	   		tempGraphs.add(tempId+"deleted");
    	   		    	   		    			
    			String query = "INSERT { GRAPH <"+getDiachronDictionaryURI()+"> {" +
    					"<"+dataset.getId()+"> <"+DiachronOntology.addedGraph+"> <"+ tempId+"added"+"> ; "
    							+ "<"+DiachronOntology.deletedGraph+"> <"+ tempId+"deleted"+">" +
    				"} } ";			
    			
    			VirtGraph virt = StoreConnection.getVirtGraph();
    			
    			VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, virt);
    			
    			vur.exec();
    			
    			dataset.setDeltaGraphs(tempId+"added", tempId+"deleted");
    			
    	   }    	       	   
    	   
    	   return returnMap;
		
       }
      
       public ElementGroup addFakeQueryElements(ElementGroup fakeMatGroup, HashMap<Dataset, String[]> matMap, ElementGroup inputPattern){
    	       	   
    	   
    	   for(Dataset dataset : matMap.keySet()){
			
    		   		    		   		
					String rsURI = dict.getDataset(dataset.getLastFullyMaterialized()).getRecordSet().getId();
					
					DiachronQuery subQuery = new DiachronQuery();
					
					SPARQLParser parser = new DiachronParserSPARQL11();
					
					/*boolean mergeSources[] = mergeGraphSourceSelection(inputPattern, rsURI, matMap.get(dataset)[0]);
					
					String qs = " SELECT DISTINCT * " ;
					
					if(mergeSources[0]) 
						qs += "FROM <"+rsURI+"> ";
					if(mergeSources[1])
						qs += "FROM <"+matMap.get(dataset)[0]+"> ";
					
					qs += "WHERE " + inputPattern.toString().replaceAll("\\. \\.", "\\. ");*/				
					
					String qs = " SELECT DISTINCT * " 
							+ "FROM <"+rsURI+"> "
							+ "FROM <"+matMap.get(dataset)[0]+"> "
							+ "WHERE " + inputPattern.toString().replaceAll("\\. \\.", "\\. ");	
					
					subQuery = (DiachronQuery) parser.parse( subQuery, qs);
								 
					ElementSubQuery validGraphs = new ElementSubQuery(subQuery);
					
					fakeMatGroup.addElement(validGraphs);
	    	   
					List<Element> inputList = inputPattern.getElements();
	    	     					  	    	    
	  	    	    for(Element inputElement : inputList ){
	  	    		
	  	    	       ElementMinus minusTriple;
	  	    	       
	  	    	       ElementTriplesBlock singleMinusTriple;
	  	    		   	  					  	    	      	       		       		  
	  	    		   if(inputElement.getClass().getName().toString().contains("ElementTriplesBlock")){
	  	    			   
	  	    			   ElementTriplesBlock triplePathBlock = (ElementTriplesBlock) inputElement;
	  	    			   	  	    			  	  	    			   
	  	    			   for(Triple triple : triplePathBlock.getPattern().getList()){
	  	    				
	  	    				   if(triple.getPredicate().isURI() && triple.getPredicate().getURI().toString().equals(DiachronOntology.subject.getURI().toString())) continue;

	  	    				   singleMinusTriple = new ElementTriplesBlock();
	  	    				   
	  	    				   singleMinusTriple.addTriple(triple);
	  	    				   	  	    				  		  	    	       	   
	  	    				   minusTriple = new ElementMinus(new ElementNamedGraph(NodeFactory.createURI(matMap.get(dataset)[1]), singleMinusTriple));
	  	    				   
	  	    				   fakeMatGroup.addElement(minusTriple);
	  	    				   
	  	    			   }  					  	    			  					  	    			   
	  	    		   
	  	    		   }
	  	    		   
	  	    		   else if(inputElement.getClass().getName().toString().contains("ElementPathBlock")){
	  	    			
	  	    			   ElementPathBlock triplePathBlock = (ElementPathBlock) inputElement;    			  
	  	    			   
	  	    			   for(TriplePath triplePath : triplePathBlock.getPattern().getList()){
	  	    				
	  	    				   if(triplePath.isTriple())    {					   
	  	    							  					  	    					  
		  	    					if(triplePath.asTriple().getPredicate().isURI() && triplePath.asTriple().getPredicate().getURI().toString().equals(DiachronOntology.subject.getURI().toString())) continue;
		  	    					
			  	    				   singleMinusTriple = new ElementTriplesBlock();
			  	    				   
			  	    				   singleMinusTriple.addTriple(triplePath.asTriple());
			  	    				   
			  	    				   minusTriple = new ElementMinus(new ElementNamedGraph(NodeFactory.createURI(matMap.get(dataset)[1]), singleMinusTriple));
			  	    				   
			  	    				   fakeMatGroup.addElement(minusTriple);
		  	    				   
	  	    				   }	  					  	    				   
	  	    			   
	  	    			   }  					  	    			   
	  	    			   
	  	    		   }	  					  	    		   	  	    		   
	  	    			   
	  	    	   }	  					  	    	   
					
				}
    	   
    	   return fakeMatGroup;
    	   
       }
       
       public boolean[] mergeGraphSourceSelection(ElementGroup pattern, String graph1, String graph2){
    	   
    	   //Source selection for merge of graphs
    	   VirtGraph graph =  (VirtGraph) StoreConnection.getVirtGraph();
   			
 		   graph.setReadFromAllGraphs(true);
 		   
 		   VirtuosoQueryExecution vqe ;	
 		   
 		   boolean mergeSources[] = new boolean[2];
		   
 		   mergeSources[0] = true; mergeSources[1] = true;
 		   
 		   ElementTriplesBlock tripleElement ;
		   		   		   
 		   for(Element inputElement : pattern.getElements() ){
 	    			    	       	    		   	  					  	    	      	       		       		  
	    		   if(inputElement.getClass().getName().toString().contains("ElementTriplesBlock")){
	    			   
	    			   ElementTriplesBlock triplePathBlock = (ElementTriplesBlock) inputElement;		
	    			   
	    			   for(Triple triple : triplePathBlock.getPattern().getList()){
	    				   
	    				   if(triple.getPredicate().isURI()){
	    					
	    					   if(triple.getPredicate().getURI().toString().equals(DiachronOntology.hasRecordAttribute.getURI().toString()) || triple.getPredicate().getURI().toString().equals(DiachronOntology.subject.getURI().toString()))
	    						
	    						   continue;
	    				   }
	    				   //if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) continue;
	    				   
	    				   if(triple.getSubject().isVariable() && triple.getObject().isVariable()) continue;//for DIACHRON model
	    			   			 
	    					   String query ;
	    					   
	    					   tripleElement = new ElementTriplesBlock();
	    					   
	    					   tripleElement.addTriple(triple);
	    					   
	    		     		   if(mergeSources[0]){
	    		     			  query = " ASK "    		   		   		
	    			       		   		   	+ " {"
	    			       		   		   	+ "		GRAPH <"+graph1+"> {" 
	    			       		   		   	+ 			tripleElement.toString().replaceAll("\\. \\.", "\\. ")
	    			       		   		   	+" }}";
	    		     			  System.out.println(query);
		    		     		   vqe = VirtuosoQueryExecutionFactory.create(query, graph);
		    		 	       	  
		    		 	       	   if(!vqe.execAsk()) {
		    		 	       		   mergeSources[0] = false;
		    		 	       	   }
		    		 	       	   vqe.close();
	    		     		   }
	    		 			    
	    		 	       	   
	    		     		  if(mergeSources[1]){
	    		     			  query = " ASK "    		   		   		
	    			       		   		   	+ " {"
	    			       		   		   	+ "		GRAPH <"+graph2+"> {" 
	    			       		   		   	+ 			tripleElement.toString().replaceAll("\\. \\.", "\\. ")
	    			       		   		   	+" }}";
	    		     			 System.out.println(query);
		    		     		   vqe = VirtuosoQueryExecutionFactory.create(query, graph);
		    		 	       	  
		    		 	       	   if(!vqe.execAsk()) {
		    		 	       		   mergeSources[1] = false;
		    		 	       	   }
		    		 	       	   vqe.close();
	    		     		   }	    				   
	    			   }
	    		   }
	    		   else if(inputElement.getClass().getName().toString().contains("ElementPathBlock")){
	  	    			
  	    			   ElementPathBlock triplePathBlock = (ElementPathBlock) inputElement;    			  
  	    			   
  	    			   for(TriplePath triplePath : triplePathBlock.getPattern().getList()){
  	    				
  	    				   if(triplePath.isTriple())    {
  	    					   
  	    					   Triple triple = triplePath.asTriple();
  	    					   
  	    					   if(triple.getPredicate().isURI() ) {
  	    						 
  	    						 if(triple.getPredicate().getURI().toString().equals(DiachronOntology.hasRecordAttribute.getURI().toString()) || triple.getPredicate().getURI().toString().equals(DiachronOntology.subject.getURI().toString()))
  	    						 //if(triple.getPredicate().getURI().toString().equals(DiachronOntology.hasRecordAttribute.getURI().toString()) ) //|| triple.getPredicate().getURI().toString().equals(DiachronOntology.subject.getURI().toString())
  	    							   continue;
  	    					   }
  	    					   
  	    					 if(triple.getSubject().isVariable() && triple.getObject().isVariable()) continue;
  	    					 //if(triple.getSubject().isVariable() && triple.getPredicate().isVariable() && triple.getObject().isVariable()) continue;
  	    					 
	    			   			 
  	    					   String query ;
  	    					   
  	    					   tripleElement = new ElementTriplesBlock();
	    					   
	    					   tripleElement.addTriple(triple);
  	    					   
  	    		     		   if(mergeSources[0]){
  	    		     			  query = " ASK "    		   		   		
  	    			       		   		   	+ " {"
  	    			       		   		   	+ "		GRAPH <"+graph1+"> {" 
  	    			       		   		   	+ 			tripleElement.toString().replaceAll("\\. \\.", "\\. ")
  	    			       		   		   	+" }}";
  	    		     			System.out.println(query);
  		    		     		   vqe = VirtuosoQueryExecutionFactory.create(query, graph);
  		    		 	       	  
  		    		 	       	   if(!vqe.execAsk()) {
  		    		 	       		   mergeSources[0] = false;
  		    		 	       	   }
  		    		 	       	   vqe.close();
  	    		     		   }
  	    		 			    
  	    		 	       	   
  	    		     		  if(mergeSources[1]){
  	    		     			  query = " ASK "    		   		   		
  	    			       		   		   	+ " {"
  	    			       		   		   	+ "		GRAPH <"+graph2+"> {" 
  	    			       		   		   	+ 			tripleElement.toString().replaceAll("\\. \\.", "\\. ")
  	    			       		   		   	+" }}";
  	    		     			System.out.println(query);
  		    		     		   vqe = VirtuosoQueryExecutionFactory.create(query, graph);
  		    		 	       	  
  		    		 	       	   if(!vqe.execAsk()) {
  		    		 	       		   mergeSources[1] = false;
  		    		 	       	   }
  		    		 	       	   vqe.close();
  	    		     		   }
  	    		 			    
  	    		 	       	   
  	    		 	       	   
  	    		 		   
  	    				   }
  	    			   }
	    		   }
    	  }
    	      	          	  
       	   graph.close();
       	   
       	   return mergeSources;
	       //End of source selection for merge
    	   
       }
       
       public ElementGroup addFakeQueryElements(ElementGroup fakeMatGroup, HashMap<Dataset, String[]> matMap, ElementGroup inputPattern, String versionVariable, Node diachronicDataset){
    	   
    	   for(Dataset dataset : matMap.keySet()){
					
					String rsURI = dict.getDataset(dataset.getLastFullyMaterialized()).getRecordSet().getId();  					  					
					
					DiachronQuery subQuery = new DiachronQuery();
					
					SPARQLParser parser = new DiachronParserSPARQL11();
			
					subQuery = (DiachronQuery) parser.parse(subQuery, " SELECT DISTINCT * FROM <"+rsURI+"> FROM <"+matMap.get(dataset)[0]+"> WHERE " + inputPattern.toString().replaceAll("\\. \\.", "\\. "));  					  			
					
					ElementSubQuery validGraphs = new ElementSubQuery(subQuery);  					  					  					  					
					
					subQuery = new DiachronQuery();
					if(diachronicDataset.isVariable())
						
						subQuery = (DiachronQuery) parser.parse(subQuery, " SELECT (<"+dataset.getId()+"> as ?"+versionVariable+") (<"+dataset.getDiachronicDataset().getId()+"> as ?"+diachronicDataset.getName().toString()+") WHERE {}");  					  			
					else
						subQuery = (DiachronQuery) parser.parse(subQuery, " SELECT (<"+dataset.getId()+"> as ?"+versionVariable+") WHERE {}");
					
					ElementSubQuery versionBinderElement = new ElementSubQuery(subQuery);
					
					ElementGroup thisDataset = new ElementGroup();
					
					thisDataset.addElement(versionBinderElement);
					
					thisDataset.addElement(validGraphs);
					
					fakeMatGroup.addElement(thisDataset);  					  					
	    	   
					List<Element> inputList = inputPattern.getElements();
	    	     					  	    	    
	  	    	    for(Element inputElement : inputList ){
	  	    		
	  	    	       ElementMinus minusTriple;
	  	    	       
	  	    	       ElementTriplesBlock singleMinusTriple;	  					  	    	       	  					  	    	       
	  	    		   	  					  	    	      	       		       		  
	  	    		   if(inputElement.getClass().getName().toString().contains("ElementTriplesBlock")){
	  	    			   
	  	    			   ElementTriplesBlock triplePathBlock = (ElementTriplesBlock) inputElement;
	  	    			   
	  	    			   for(Triple triple : triplePathBlock.getPattern().getList()){
	  	    				
	  	    				   if(triple.getPredicate().getURI().toString().equals(DiachronOntology.subject.getURI().toString())) continue;

	  	    				   singleMinusTriple = new ElementTriplesBlock();
	  	    				   
	  	    				   singleMinusTriple.addTriple(triple);
	  	    				   
	  	    				   minusTriple = new ElementMinus(new ElementNamedGraph(NodeFactory.createURI(matMap.get(dataset)[1]), singleMinusTriple));
	  	    				   
	  	    				   fakeMatGroup.addElement(minusTriple);
	  	    				   
	  	    			   }  					  	    			  					  	    			   
	  	    		   
	  	    		   }
	  	    		   
	  	    		   else if(inputElement.getClass().getName().toString().contains("ElementPathBlock")){
	  	    			
	  	    			   ElementPathBlock triplePathBlock = (ElementPathBlock) inputElement;    			  
	  	    			   
	  	    			   for(TriplePath triplePath : triplePathBlock.getPattern().getList()){
	  	    				
	  	    				   if(triplePath.isTriple())    {					   
	  	    							  					  	    					  
		  	    					if(triplePath.asTriple().getPredicate().getURI().toString().equals(DiachronOntology.subject.getURI().toString())) continue;
		  	    					
			  	    				   singleMinusTriple = new ElementTriplesBlock();
			  	    				   
			  	    				   singleMinusTriple.addTriple(triplePath.asTriple());
			  	    				   
			  	    				   minusTriple = new ElementMinus(new ElementNamedGraph(NodeFactory.createURI(matMap.get(dataset)[1]), singleMinusTriple));
			  	    				   
			  	    				   fakeMatGroup.addElement(minusTriple);
		  	    				   
	  	    				   }	  					  	    				   
	  	    			   
	  	    			   }  					  	    			   
	  	    			   
	  	    		   }	  					  	    		   	  	    		   
	  	    			   
	  	    	   }	  					  	    	   
					
				}
    	   
    	   return fakeMatGroup;
    	   
       }
       
       public void cleanUp(){
    	   
    	   VirtGraph virt = StoreConnection.getVirtGraph();
    	   
    	   VirtuosoUpdateRequest vur;
    	   
    	   for(String tempGraph : tempGraphs){
    		   
    		   String query = "DELETE { GRAPH <"+tempGraph+"> {" +
    					"?s ?p ?o " +
    				"} } WHERE {GRAPH <"+tempGraph+"> {" +
    					"?s ?p ?o "
    				+ "}} ";
    		   
    		  
	   		   vur = VirtuosoUpdateFactory.create(query, virt);
	   		   vur.exec();
	   		   
    		   
    	   }
    	   
    	   virt.close();    	   
    	
       }
       
    public void deleteReconstructedVersion(String versionURI){
    	
    	Dataset dataset = dict.getDataset(versionURI);
    	String recordSetURI = dataset.getRecordSet().getId();
    	clearStageGraph(recordSetURI);
    	String query = "DELETE { GRAPH <"+getDiachronDictionaryURI()+"> {" +
				"<"+dataset.getId()+"> <"+DiachronOntology.isFullyMaterialized+"> ?fm " +
			"} } WHERE {GRAPH <"+getDiachronDictionaryURI()+"> {" +
				"<"+dataset.getId()+"> <"+DiachronOntology.isFullyMaterialized+"> ?fm "
			+ "}} ";
    	VirtGraph virt = StoreConnection.getVirtGraph();
		VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, virt);
		vur.exec();
		
		query = "INSERT { GRAPH <"+getDiachronDictionaryURI()+"> {" +
				"<"+dataset.getId()+"> <"+DiachronOntology.isFullyMaterialized+"> \"false\" " +
			"} } ";			
		virt = StoreConnection.getVirtGraph();
		vur = VirtuosoUpdateFactory.create(query, virt);
		vur.exec();
   		dataset.setFullyMaterialized(false);
    	
    }
       
    /**
	 * Empties the given named graph from the store
	 * @param tempGraph the graph name to be emptied
	 */
	private void clearStageGraph(String tempGraph) {
		
		Connection conn = null;
		Statement stmt = null;
		try{			
			conn = StoreConnection.getConnection();											 
		    stmt = conn.createStatement ();
		    stmt.execute ("log_enable(3,1)");
		    stmt.executeQuery("SPARQL CLEAR GRAPH <"+tempGraph+">");
		    
		}catch(Exception e){
			//logger.error(e.getMessage(), e);
			Graph graph = StoreConnection.getGraph(tempGraph);	    
			graph.clear();
			graph.close();
		}
		finally {		    
		    try { if (stmt != null) stmt.close(); } catch (Exception e) {
		    	//logger.error(e.getMessage(), e);
		    	};
		    try { if (conn != null) conn.close(); } catch (Exception e) {
		    	//logger.error(e.getMessage(), e);
		    	};
		}
		
	}
	
	public Element createDiachronChangesetInPatternElement(Node diachronicDataset, Element pattern){
		
		isFloatingQuery = false;
      	   
 	   if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;
 	   
 	   if(dict.getDiachronicDataset(diachronicDataset.getURI()) == null)
 	   {
 		   throw new QueryException("Diachronic dataset does not exist. ") ;
 	   }
 	   
 	   if(!getDiachronDatasetURIs().isEmpty() && !getDiachronChangesetURIs().containsKey(diachronicDataset.getURI()))
			  	throw new QueryException("Diachronic dataset in CHANGES clause is not specified in FROM clause.");      	   
 	   
 	   ElementGroup inputPattern = (ElementGroup) pattern;    	      	       	   
 	   
 	   HashMap<String, Node> datasetParams = diachronChangesetInPatternURIs.get(diachronicDataset);    	       	       	       	      	       	       	  	
		   
 	   ElementGroup fakeMatGroup = new ElementGroup();		   
 	  
 	   if(datasetParams != null && (datasetParams.containsKey("after") || datasetParams.containsKey("before"))){
									
						  try{
								  
								Node afterVersionURI = null;  
								if(datasetParams.containsKey("after")) {
									  									
									afterVersionURI = datasetParams.get("after");
									
									afterVersionURI = DiachronQueryUtils.getCleanNode(afterVersionURI );
									
									if(dict.getDataset(afterVersionURI.getURI())==null)	{	  			    	   		  			    	   
			  			    		   throw new QueryException("Dataset version does not exist. ") ;
			  			    	   }
								}
							  	
								Node beforeVersionURI = null;
								
								if(datasetParams.containsKey("before")) {
									  									
									beforeVersionURI = datasetParams.get("before");
									
									beforeVersionURI = DiachronQueryUtils.getCleanNode(beforeVersionURI );
									
									if(dict.getDataset(beforeVersionURI.getURI())==null)	{	  			    	   		  			    	   
			  			    		   throw new QueryException("Dataset version does not exist. ") ;
			  			    	   }
								}
								
								
								ExprList inlist = new ExprList();
		  					  																				
			  					for(Dataset dataset : dict.getListOfDatasets(dict.getDiachronicDataset(diachronicDataset.getURI()))){
			  					  		
			  						if(afterVersionURI != null && !isAfter(dataset, afterVersionURI) ) continue ;
			  						
			  						if(beforeVersionURI != null && !isBefore(dataset, beforeVersionURI) ) continue ;
			  					  	
			  						if(beforeVersionURI != null && dataset.getId().equals(beforeVersionURI.getURI())) continue;				  					  					  					  		
			  						
			  					  	String csURI = dataset.getChangeSetOld();
			  					  	
			  						
				  				 	if(!getDiachronChangesetURIs().isEmpty() && !getDiachronChangesetURIs().get(diachronicDataset.getURI()).get("after").equals(csURI))
			  							continue;
				  					  	
				  				 	if(csURI != null)	
			  					  		inlist.add(ExprUtils.nodeToExpr(NodeFactory.createURI(csURI)));			  					  			  					  					  					  								  					  					  					  					  			
		  					  				  					  			  					  		
		  					  		}
			  							
				  					if(!inlist.isEmpty()){
				  						  
				  					  String var_full_mat_uri = DiachronQueryUtils.getNextVariable();
					  					  
					  				  Expr e = new E_OneOf(new ExprVar(var_full_mat_uri), inlist);
				  					  	  
					  				  ElementFilter filter = new ElementFilter(e);					  					  					  					  
					  					  
					  				  ElementGroup fullDatasets = new ElementGroup();					  					  					  					  			  					  					  					  					  					  					  				
					  					  
					  				  fullDatasets.addElement(filter);
					  					  
					  				  fullDatasets.addElement(new ElementNamedGraph(NodeFactory.createVariable(var_full_mat_uri), inputPattern));					  					  					  					  
					  					  
					  					  //fakeMatGroup.addElement(variableSourceUnion);
					  				  fakeMatGroup.addElement(fullDatasets);
				  						  
				  					}				  					  				  					  				  					  				  					  				  					 								  							
									
				  					if(afterVersionURI!=null && afterVersionURI.isVariable() && beforeVersionURI!=null && beforeVersionURI.isVariable()){
										throw new QueryException("Temporal operators (BEFORE, AFTER, BETWEEN) cannot be followed by var.");
									}
	  								  
	  							  }
	  							  catch(QueryException e){ 
									  throw e;
								  }
	  							  catch(Exception e){ 
	  								  e.printStackTrace();
	  							  }
							 
					}
			return fakeMatGroup;
		
	}
    
	public Element createDiachronChangesetInPatternElement_old(Node diachronicDataset, Element pattern){
    	   //TODO
    	   //System.out.println(datasetURI + ": " + diachronChangesetInPatternURIs.get(datasetURI) + " " + pattern.toString());
    	   if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;   		    	   
    	   HashMap<String, Node> datasetParams = diachronChangesetInPatternURIs.get(diachronicDataset);    	   
    	   
    	   ArrayList<ElementGroup> unionList = new ArrayList<ElementGroup>();      	   
    	   String csVariable = DiachronQueryUtils.getNextVariable();
  	       ElementGroup elg_1 = new ElementGroup();	  		  			
  		   ElementTriplesBlock triples = new ElementTriplesBlock();  		     		   
  		   if(datasetParams != null )
  					{
  					  	  
  					  	        String nextVar = DiachronQueryUtils.getNextVariable();					  	        			  	          														
  								Triple t1 = new Triple(diachronicDataset, 
  					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
  					 			   NodeFactory.createVariable(nextVar));  
  								Triple t2 = new Triple(diachronicDataset, 
  	  					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasChangeSet"), 
  	  					 			   NodeFactory.createVariable(csVariable));  		
  					 			String dateVar1 = DiachronQueryUtils.getNextVariable();	
  								Triple t2date1 = new Triple(NodeFactory.createVariable(nextVar), 
  							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
  							 			   NodeFactory.createVariable(dateVar1));
  								ExprList l1 = new ExprList();
  						 	    l1.add(new ExprVar(dateVar1));
  						 	    triples.addTriple(t1);
  						 	    triples.addTriple(t2);	
  						 	 //if(datasetParams != null && (datasetParams.containsKey("after") || datasetParams.containsKey("before")))
					    			
  						 	   
  					    if(datasetParams != null && (datasetParams.containsKey("after") || datasetParams.containsKey("before"))){
  					    	triples.addTriple(t2date1);
  						  if(datasetParams.containsKey("after"))
  							{
  								Node afterVersionURI = datasetParams.get("after");
  								afterVersionURI = DiachronQueryUtils.getCleanNode(afterVersionURI ); 
  								String dateVar2 = DiachronQueryUtils.getNextVariable();
  								Triple t2date2 = new Triple(afterVersionURI, 
  							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
  							 			   NodeFactory.createVariable(dateVar2));
  							 	triples.addTriple(t2date2);
  							 	ExprList l2 = new ExprList();
  						 	    l2.add(new ExprVar(dateVar2));
  						 	      
  							 	ElementFilter afterFilter = new ElementFilter(new E_GreaterThanOrEqual(new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l1), new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l2)));
  							 	elg_1.addElement(afterFilter);		   							 								   					    		
  								
  							}
  						  if(datasetParams.containsKey("before"))
  							{
  							  	Node beforeVersionURI = datasetParams.get("before");
  							    beforeVersionURI = DiachronQueryUtils.getCleanNode(beforeVersionURI);
  							  	String dateVar2 = DiachronQueryUtils.getNextVariable();
  							  	Triple t2date2 = new Triple(beforeVersionURI, 
  							 			   NodeFactory.createURI(DiachronOntology.creationTime.getURI()), 
  							 			   NodeFactory.createVariable(dateVar2));
  							 	triples.addTriple(t2date2);
  							 	ExprList l2 = new ExprList();
  						 	    l2.add(new ExprVar(dateVar2));												 	    
  							 	ElementFilter beforeFilter = new ElementFilter(new E_LessThanOrEqual(new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l1), new E_Function("http://www.w3.org/2001/XMLSchema#dateTime", l2)));
  							 	elg_1.addElement(beforeFilter);		   							 	
  							}   							
  					  	  }
  					    elg_1.addElement(triples);
  					  	 
  					  	  
  					}
  				unionList.add(elg_1);											  		
  		  			
  				
  	       	if(unionList.size() > 1)
  	       	{
  	       	  
  				ElementUnion elu = new ElementUnion();
  				for(ElementGroup elg : unionList)
  				{
  					elu.addElement(elg);
  				}
  				//setDiachronDictionaryElements(new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), elu));
  				//return elu;
  				ElementGroup eg = new ElementGroup();
  				eg.addElement(elu);
  				eg.addElement(new ElementNamedGraph(NodeFactory.createVariable(csVariable), pattern));
  				return new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), eg);
  			}
  			else if(unionList.size()==1)
  			{			  
  				ElementGroup elu = unionList.get(0);
  				ElementGroup eg = new ElementGroup();
  				eg.addElement(elu);
  				eg.addElement(new ElementNamedGraph(NodeFactory.createVariable(csVariable), pattern));
  				//setDiachronDictionaryElements(new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), elu));
  				return new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), eg);
  			}
  			
    	   return null;
       }
       
       public ElementPathBlock finalizeRecordAttributePattern(Node subject, ElementPathBlock epb, Node recordNode, ArrayList<Node[]> poPairs){
    	   
    	   Triple t;
    	   for(Node[] po : poPairs)
    		  {
    			t = new Triple(recordNode, po[0], po[1]);
    			epb.addTriple(t);
    		  }
    	   return epb;
       }
       
       public Element createDiachronRecordInPatternElement(Node recordURI, Element pattern){
    	   //TODO
    	   //System.out.println(recordURI + ": " +pattern.toString());
    	   //if(true) return null;
    	   ElementTriplesBlock triples = new ElementTriplesBlock();
    	   Node recordNode ;
    	   boolean isSetSubject = false;
    	   recordNode = DiachronQueryUtils.getCleanNode(recordURI);
    	  /* if (recordURI.equals("")){
    		   recordNode = NodeFactory.createAnon();
    	   }
    	   else if(recordURI.startsWith("_var_")){
    		   recordNode = NodeFactory.createVariable(recordURI.replaceAll("_var_", ""));
    	   }
    	   else{    		   
    		   recordNode = NodeFactory.createURI(recordURI);
    	   }*/
    	   Node subject = null, predicate = null, object = null;
    	   //ElementGroup etb = (ElementGroup) pattern;
    	   //for(Element el : etb.getElements()){
    		   
    		   ElementPathBlock epb = (ElementPathBlock) pattern;
    		   
    		   for(TriplePath tp : epb.getPattern().getList()){
    			   //System.out.println("hello " + tp.toString());
    			   //subject = NodeFactory.createURI(tp.getSubject().toString());
    			   Node recAtt = null; 
    			   if(!attributeMap.containsKey(tp) || attributeMap.get(tp)==null) 		
    				   recAtt = NodeFactory.createVariable(DiachronQueryUtils.getNextVariable());
    			   else
    				   recAtt = DiachronQueryUtils.getCleanNode(attributeMap.get(tp));
    			   predicate = DiachronQueryUtils.getCleanNode(tp.getPredicate());
    			   object = DiachronQueryUtils.getCleanNode(tp.getObject());
    			   //Node recAtt ;
    			   
    	    	   
    	    	   Triple t1 = new Triple(recordNode,  
    	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasRecordAttribute"), 
    	    			   recAtt);    	
    	    	   triples.addTriple(t1);
    	    	   if(!isSetSubject) {
    				   subject = DiachronQueryUtils.getCleanNode(tp.getSubject());
    				   Triple t4 = new Triple(recordNode, 
        	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/subject"), 
        	    			   subject);
    				   triples.addTriple(t4);
        			   isSetSubject = true;
    			   } 
    	    	   Triple t2 = new Triple(recAtt, 
    	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/predicate"),  
    	    			   predicate);
    	    	   Triple t3 = new Triple(recAtt, 
    	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/object"),  
    	    			   object);
    	    	   
    	    	   
    	    	   triples.addTriple(t2);
    	    	   triples.addTriple(t3);
    	     
    		   }
    			   
    		   
    	   //}
    	   
    	   //System.out.println("Triples: " + triples.toString());
    	   //System.out.println(triples);
    	   return triples;
       }
       
       public ElementGroup buildDiachronQuery(){
    	   ElementGroup g = new ElementGroup();
    	   //System.out.println(getDiachronDictionaryElements());
    	   if(getDiachronDictionaryElements() != null) {
    		   
    		   if(isFloatingQuery) {
    			   
    			   g.addElement(getDiachronDictionaryElements());
    			   
        		   if(getDiachronChangesetURIs().isEmpty())
        			   g.addElement(new ElementNamedGraph(NodeFactory.createVariable("rs"), getDiachronQueryBody()));
        		   
        		   else
        			   g.addElement(new ElementNamedGraph(NodeFactory.createVariable("cs"), getDiachronQueryBody()));
        		   
    		   }
    		   
    		   else {    		   
        		   g.addElement(getDiachronQueryBody());
        	   }
    		   
    	   }
    	   else{    		   
    		   g.addElement(getDiachronQueryBody());
    	   }
    	   //g.addElement(getDiachronQueryBody());
    	   
    	   //System.out.println(getDiachronQueryBody());    	  
    	   return g;
       }
       
       
              
       

}
