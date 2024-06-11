package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

@Component
public class DeletionModule {

    private static final Logger logger = LoggerFactory.getLogger(DeletionModule.class);

    private final DbOperations dbOperations;
    private final AppConfig appConfig;

    public DeletionModule(DbOperations dbOperations, AppConfig appConfig) {
        this.dbOperations = dbOperations;
        this.appConfig = appConfig;
    }

    public void processDeletionFile(String filePath) throws IOException, SQLException {
        logger.info("Processing deletion file: {}", filePath);
        try (CSVParser parser = new CSVParser(new FileReader(filePath), CSVFormat.DEFAULT.withHeader())) {
            Map<String, Integer> headerRecord = parser.getHeaderMap();
            logger.info("Headers detected by CSVParser: {}", headerRecord);

            for (CSVRecord record : parser) {
                String msisdn = record.get(appConfig.getMsisdnColumn());
                try {
                    // Log record details
                    logger.info("Processing deletion for MSISDN: {}", msisdn);

                    // Check if msisdn is not empty
                    if (msisdn == null || msisdn.trim().isEmpty()) {
                        logger.error("Error: MSISDN value is empty.");
                        System.exit(1);
                    }

                    // Delete record from the database
                    dbOperations.deleteRecord(msisdn);

                    // Log success message
                    logger.info("Deletion for MSISDN {} successful.", msisdn);
                } catch (SQLException e) {
                    // Log error message if there's an issue deleting the record
                    logger.error("Error deleting record for MSISDN {}: {}", msisdn, e.getMessage());
                    System.exit(1); // Exit if deletion fails
                }
            }
        } catch (IOException e) {
            // Log error message if there's an issue reading the file
            logger.error("Error reading file: {}", e.getMessage());
            throw e; // Rethrow the exception to propagate it
        }
    }
}
