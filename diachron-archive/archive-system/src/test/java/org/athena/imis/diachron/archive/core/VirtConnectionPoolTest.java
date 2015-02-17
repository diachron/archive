package org.athena.imis.diachron.archive.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.athena.imis.diachron.archive.api.DataStatement;
import org.athena.imis.diachron.archive.api.StatementFactory;
import org.athena.imis.diachron.archive.core.dataloader.ArchiveEntityMetadata;
import org.athena.imis.diachron.archive.core.dataloader.DictionaryCache;

import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDFS;

public class VirtConnectionPoolTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DictionaryCache.init();
		DataStatement dataStatement = StatementFactory.createDataStatement();
		
		ArchiveEntityMetadata metadata = new ArchiveEntityMetadata();
		HashMap<String, String> metadataMap = new HashMap<String, String>();
		metadataMap.put(RDFS.label.toString(), "label");
		metadataMap.put(DCTerms.creator.toString(), "creator");		
		metadata.setMetadataMap(metadataMap);
		
		try {
			for (int i = 0; i< 10; i++) {
				String datasetName = "TESTPOOL" + (new Date()).getTime() +""+(int)(100000.0*Math.random());
				dataStatement.createDiachronicDataset(metadata, datasetName);
				loadDatasets(dataStatement);
				//dataStatement.loadData(input, diachronicDatasetURI)createDataset(metadata, datasetName);
				System.out.println("Done:"+i);
				Thread.sleep(1000);
				
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void loadDatasets(DataStatement vds) throws IOException{
		String dir = "C:/Users/Marios/Documents/Projects/DIACHRON/EBI/toLoad/";
		File[] files = new File(dir).listFiles();		
		String[] labels = new String[files.length];
		int i = -1;		
		for(File file : files){
			i++;
			labels[i] = file.getName();
		}
		//System.out.println(Arrays.toString(labels));		
		i = -1;
		for(File file : files){
			i++;			
			//System.out.println("Loading file " + file.getName());
			File inputFile = new File(dir+file.getName());
			if(!inputFile.getName().contains("_diachron")) continue;
			FileInputStream fis = null;						
			try {
				long tStart = System.currentTimeMillis();
				fis = new FileInputStream(inputFile);
				//DataStatement vds = StatementFactory.createDataStatement();				
				vds.loadData(fis, "http://www.diachron-fp7.eu/resource/diachronicDataset/efo/475B9ABBF2FA36351EE30C79F440719B");			
				//vds.loadData(fis, "http://www.diachron-fp7.eu/resource/diachronicDataset/4e58d4");
				fis.close();								
				
			} catch (IOException e) {
				e.printStackTrace();				
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}	
		
	}
	
}
