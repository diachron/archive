package org.athena.imis.diachron.archive.datamapping.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.athena.imis.diachron.archive.datamapping.MultidimensionalConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestConversionFile {

    private static final Logger logger = LoggerFactory.getLogger(TestConversionFile.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        MultidimensionalConverter converter = new MultidimensionalConverter();

        File inputFile = new File("/tmp/geo.ttl");
        File outputFile = new File(inputFile.getAbsolutePath() + ".converted");
        System.out.println("Converting file " + inputFile.getName());

        try (
                FileInputStream fis = new FileInputStream(inputFile);
                FileOutputStream fos = new FileOutputStream(outputFile, false)
        ) {
            converter.convert(fis, fos, "ttl", "test");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        System.exit(1);
    }
}
