package org.athena.imis.diachron.archive.datamapping.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.athena.imis.diachron.archive.datamapping.OntologyConverter;

public class TestConversionFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String dir = "C:/Users/Marios/Documents/Projects/DIACHRON/EBI/v6/efo-last-15-owl/toConvert/";
		File[] files = new File(dir).listFiles();
		for(File file : files){
			
			System.out.println("Converting file " + file.getName());
			File inputFile = new File(dir+file.getName());
			FileInputStream fis = null;
			File outputFile = new File(dir+"_diachron_"+file.getName());
			
			try {
				fis = new FileInputStream(inputFile);
				FileOutputStream fos = new FileOutputStream(outputFile);
				OntologyConverter converter = new OntologyConverter();
				converter.convert(fis, fos);
				fis.close();
				fos.close();
	 
			} catch (IOException e) {
				e.printStackTrace();				
			}
		}
		
		
		

	}
	
	
}
