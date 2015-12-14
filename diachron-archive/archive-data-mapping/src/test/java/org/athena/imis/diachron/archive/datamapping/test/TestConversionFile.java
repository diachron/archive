package org.athena.imis.diachron.archive.datamapping.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.athena.imis.diachron.archive.datamapping.RDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestConversionFile {

	private static final Logger logger = LoggerFactory.getLogger(TestConversionFile.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//String dir = "C:/Users/Marios/Documents/Projects/DIACHRON/EBI/v6/efo-last-15-owl/toConvert/";
		String dir = args[0];
		File[] files = new File(dir).listFiles();
		for(File file : files){
			
			System.out.println("Converting file " + file.getName());
			
			File inputFile = new File(dir+file.getName());
			FileInputStream fis = null;
			File outputFile = new File(dir+"d_diachron_"+file.getName()+".rdf");
			
			try {
				fis = new FileInputStream(inputFile);
				FileOutputStream fos = new FileOutputStream(outputFile);
				//OntologyConverter converter = new OntologyConverter();
				RDFConverter converter = new RDFConverter();
				//MultidimensionalConverter converter = new MultidimensionalConverter();
				//converter.convert(fis, fos, file.getName().substring(file.getName().lastIndexOf(".")+1), "test_qb_data");
				converter.convert(fis, fos, "sdsd", file.getName().substring(file.getName().lastIndexOf(".")+1));
				fis.close();
				fos.close();
	 
			} catch (IOException e) {
				logger.error(e.getMessage(), e);			
			}
		}
		
		
		

	}
	
	
}
