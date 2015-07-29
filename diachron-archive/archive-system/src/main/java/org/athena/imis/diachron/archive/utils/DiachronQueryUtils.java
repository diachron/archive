package org.athena.imis.diachron.archive.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.athena.imis.diachron.archive.core.dataloader.DictionaryService;
import org.athena.imis.diachron.archive.core.dataloader.StoreFactory;
import org.athena.imis.diachron.archive.lang.DiachronQuery;
import org.athena.imis.diachron.archive.models.DiachronicDataset;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.AnonId;

public class DiachronQueryUtils {
		 
	
	 private static int varCounter = 0;
	
	
	 public static String getNextVariable(){ return "var"+(varCounter++);}
	 
	 private static DictionaryService dict = StoreFactory.createDictionaryService();
	
	 public static HashMap<String, Node> getDatasetParameters(HashMap<String, Node> datasetParams, HashMap<String, Node> varDatasetParams){
    		          	      	   
  	   if(datasetParams == null) {
  		           		           		   
  		   if(varDatasetParams != null) {
  			   
  			   datasetParams = varDatasetParams;
  			   
  		   }
  		   else{
  			   
  			   datasetParams = new HashMap<>();
  			   
  			   datasetParams.put("at", NodeFactory.createVariable(getNextVariable()));
  			   
  		   }
  			   	           		      		   
  	   }
  	   
  	   return datasetParams;    	   
  	   
     }
	 
	 
	 public static Node getCleanNode(Node node){
		
		 if(node == null ) //node = NodeFactory.createAnon();
  		   node = NodeFactory.createVariable(DiachronQueryUtils.getNextVariable());
  	   
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
	 
	 public static List<Node> getListOfDiachronicDatasets(Node diachronicDataset){
		
		 List<Node> diachronicList = new ArrayList<>();
  	   
	  	   if(!diachronicDataset.isVariable()){
	  		       		      		     		       		   
	  		    diachronicList.add(diachronicDataset);
	  		    
	  	   }
	  	   
	  	   else{
	  		   
	  		   for(DiachronicDataset nextDiachronic : dict.getListOfDiachronicDatasets()){
	  			   
	  			   diachronicList.add(NodeFactory.createURI(nextDiachronic.getId()));
	  		   }
	  		   
	  	   }
		 return diachronicList;
		 
	 }

}
