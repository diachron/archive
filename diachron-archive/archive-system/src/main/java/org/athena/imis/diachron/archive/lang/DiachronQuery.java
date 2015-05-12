package org.athena.imis.diachron.archive.lang;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.athena.imis.diachron.archive.core.dataloader.RDFDictionary;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;


public class DiachronQuery extends Query {
	
    //Diachron Elements
  
    private Map<String, HashMap<String, String>> diachronDatasetURIs = new HashMap<String, HashMap<String, String>>();
    private Map<String, HashMap<String, String>> diachronChangesetURIs = new HashMap<String, HashMap<String, String>>();
    private Map<Node, HashMap<String, Node>> diachronDatasetInPatternURIs = new HashMap<Node, HashMap<String, Node>>();
    private Map<String, HashMap<String, String>> diachronChangesInPatternURIs = new HashMap<String, HashMap<String, String>>();
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
    //private String diachronDictionaryURI = "http://example.com/dictionary";
    public String getDiachronDictionaryURI() { return RDFDictionary.getDictionaryNamedGraph(); }
    private int varCounter = 0;
    public String getNextVariable(){ return "var"+(varCounter++);}
    
    
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
    	//Node is already cleaned
    	//diachronicDataset = getCleanNode(diachronicDataset);
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
   					  	  
   					  	        String nextVar = getNextVariable();					  	        			  	          														
   								Triple t1 = new Triple(NodeFactory.createURI(iri), 
   					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
   					 			   NodeFactory.createVariable(nextVar));
   					 			Triple t2 = new Triple(NodeFactory.createVariable(nextVar), 
   							 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasRecordSet"), 
   							 			   NodeFactory.createVariable("rs"));
   					 			
   					 			String dateVar1 = getNextVariable();	
   								Triple t2date1 = new Triple(NodeFactory.createVariable(nextVar), 
   							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
   								String dateVar2 = getNextVariable();
   								Triple t2date2 = new Triple(NodeFactory.createURI(afterVersionURI), 
   							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
   							  	String dateVar2 = getNextVariable();
   							  	Triple t2date2 = new Triple(NodeFactory.createURI(beforeVersionURI), 
   							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
   		  		     
   		  		    String nextVar = getNextVariable();			
   		  		    Triple t1 = new Triple(NodeFactory.createURI(iri), 
			 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
			 			   NodeFactory.createVariable(nextVar));
			 		Triple t2 = new Triple(NodeFactory.createVariable(nextVar), 
					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasChangeSet"), 
					 			   NodeFactory.createVariable("cs"));
			 		triples.addTriple(t1);
			    	triples.addTriple(t2);
   					if(datasetParams != null && (datasetParams.containsKey("after") || datasetParams.containsKey("before")))
   					  	  {
   					  	      		  	        			  	       
   								String dateVar1 = getNextVariable();								
   								
   								Triple t2date1 = new Triple(NodeFactory.createVariable(nextVar), 
   							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
   							 			   NodeFactory.createVariable(dateVar1));
   							 						   							 			   
   								ExprList l1 = new ExprList();
   						 	    l1.add(new ExprVar(dateVar1));   						 	      						 	      							 	
   					    		triples.addTriple(t2date1);					    		   					    					    	
   						  if(datasetParams.containsKey("after"))
   							{
   								String afterVersionURI = datasetParams.get("after");
   								String dateVar2 = getNextVariable();
   								Triple t2date2 = new Triple(NodeFactory.createURI(afterVersionURI), 
   							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
   							  	String dateVar2 = getNextVariable();
   							  	Triple t2date2 = new Triple(NodeFactory.createURI(beforeVersionURI), 
   							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
    	   //TODO
    	   System.out.println(diachronicDataset + ": " + diachronDatasetInPatternURIs.get(diachronicDataset) + " " + pattern.toString());
    	   
    	   if(!isDiachronQuery()) throw new QueryException("Query is not initialized as a DIACHRON query. ") ;   		    	   
    	   HashMap<String, Node> datasetParams = diachronDatasetInPatternURIs.get(diachronicDataset);
    	   System.out.println("Params: " + datasetParams.toString());
    	   
    	   ArrayList<ElementGroup> unionList = new ArrayList<ElementGroup>();  	       	

  	       ElementGroup elg_1 = new ElementGroup();	  		  			
  		   ElementTriplesBlock triples = new ElementTriplesBlock();
  		   ElementTriplesBlock triplesOpt = new ElementTriplesBlock();   		  		    
  		   if(datasetParams != null && datasetParams.containsKey("at")) //First priority
  					{
  					  	Node atVersionURI = datasetParams.get("at");
  					  	atVersionURI = getCleanNode(atVersionURI);
  						Triple t1 = new Triple(diachronicDataset, 
  			 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
  			 			   atVersionURI);
  			 			Triple t2 = new Triple(atVersionURI, 
  					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasRecordSet"), 
  					 			   NodeFactory.createVariable("rs"));
  					 	if(!getSchemaDisabled())
  					 	{
  						   Triple t3 = new Triple(atVersionURI, 
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
  					  	  
  					  	        String nextVar = getNextVariable();					  	        			  	          														
  								Triple t1 = new Triple(diachronicDataset, 
  					 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasInstantiation"), 
  					 			   NodeFactory.createVariable(nextVar));
  					 			Triple t2 = new Triple(NodeFactory.createVariable(nextVar), 
  							 			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasRecordSet"), 
  							 			   NodeFactory.createVariable("rs"));
  					 			
  					 			String dateVar1 = getNextVariable();	
  								Triple t2date1 = new Triple(NodeFactory.createVariable(nextVar), 
  							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
  								Node afterVersionURI = datasetParams.get("after");
  								afterVersionURI = getCleanNode(afterVersionURI ); 
  								String dateVar2 = getNextVariable();
  								Triple t2date2 = new Triple(afterVersionURI, 
  							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
  							    beforeVersionURI = getCleanNode(beforeVersionURI);
  							  	String dateVar2 = getNextVariable();
  							  	Triple t2date2 = new Triple(beforeVersionURI, 
  							 			   NodeFactory.createURI("http://purl.org/dc/terms/date"), 
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
  				eg.addElement(new ElementNamedGraph(NodeFactory.createVariable("rs"), pattern));
  				return new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), eg);
  			}
  			else if(unionList.size()==1)
  			{			  
  				ElementGroup elu = unionList.get(0);
  				ElementGroup eg = new ElementGroup();
  				eg.addElement(elu);
  				eg.addElement(new ElementNamedGraph(NodeFactory.createVariable("rs"), pattern));
  				//setDiachronDictionaryElements(new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), elu));
  				return new ElementNamedGraph(NodeFactory.createURI(getDiachronDictionaryURI()), eg);
  			}
  			
    	   return null;
       }
       
       public Element createDiachronChangesInPatternElement(String datasetURI, Element pattern){
    	   //TODO
    	   System.out.println(datasetURI + ": " + diachronChangesInPatternURIs.get(datasetURI) + " " + pattern.toString());
    	   return null;
       }
       
       public Element createDiachronRecordInPatternElement(Node recordURI, Element pattern){
    	   //TODO
    	   System.out.println(recordURI + ": " +pattern.toString());
    	   //if(true) return null;
    	   ElementTriplesBlock triples = new ElementTriplesBlock();
    	   Node recordNode ;
    	   boolean isSetSubject = false;
    	   recordNode = getCleanNode(recordURI);
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
    			    			   
    			   predicate = getCleanNode(tp.getPredicate());
    			   object = getCleanNode(tp.getObject());
    			   Node recAtt = NodeFactory.createAnon(new AnonId(getNextVariable()));
    	    	   
    	    	   Triple t1 = new Triple(recordNode,  
    	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/hasRecordAttribute"), 
    	    			   recAtt);    	   
    	    	   Triple t2 = new Triple(recAtt, 
    	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/predicate"),  
    	    			   predicate);
    	    	   Triple t3 = new Triple(recAtt, 
    	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/object"),  
    	    			   object);
    	    	   
    	    	   triples.addTriple(t1);
    	    	   triples.addTriple(t2);
    	    	   triples.addTriple(t3);
    	    	   
    	    	   if(!isSetSubject) {
    				   subject = getCleanNode(tp.getSubject());
    				   Triple t4 = new Triple(recordNode, 
        	    			   NodeFactory.createURI("http://www.diachron-fp7.eu/resource/subject"), 
        	    			   subject);
    				   triples.addTriple(t4);
        			   isSetSubject = true;
    			   }   
    	    	   
    		   }
    			   
    		   
    	   //}
    	   
    	   /*System.out.println("Triples:");
    	   System.out.println(triples);*/
    	   return triples;
       }
       
       public ElementGroup buildDiachronQuery(){
    	   ElementGroup g = new ElementGroup();
    	   //System.out.println(getDiachronDictionaryElements());
    	   if(getDiachronDictionaryElements() != null) {
    		   g.addElement(getDiachronDictionaryElements());
    		   g.addElement(new ElementNamedGraph(NodeFactory.createVariable("rs"), getDiachronQueryBody()));
    	   }
    	   else{    		   
    		   g.addElement(getDiachronQueryBody());
    	   }
    	   //g.addElement(getDiachronQueryBody());
    	   
    	   //System.out.println(getDiachronQueryBody());    	  
    	   return g;
       }
       
       public Node getCleanNode(Node node){
    	   if(node == null ) node = NodeFactory.createAnon();
    	   else if(node.isURI())
    		   node = NodeFactory.createURI(node.getURI());
		   else if(node.isLiteral())
			   node = NodeFactory.createLiteral(node.getLiteral());
		   else if(node.isVariable())
			   node = NodeFactory.createVariable(node.getName());
		   else if(node.isBlank())
			   node = NodeFactory.createAnon(new AnonId(node.getBlankNodeLabel()));
    	   return node;
       }
              
       

}
