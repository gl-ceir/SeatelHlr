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
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class AdditionModule {

    private static final Logger logger = LoggerFactory.getLogger(AdditionModule.class);

    private final DbOperations dbOperations;
    private final AppConfig appConfig;

    @Value("${fixed.operator.value}")
    private String fixedOperatorValue;

    public AdditionModule(DbOperations dbOperations, AppConfig appConfig) {
        this.dbOperations = dbOperations;
        this.appConfig = appConfig;
    }

    public void processAdditionFile(String filePath, Date date) throws IOException, SQLException {
        logger.info("Processing addition file: {}", filePath);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String formattedDate = sdf.format(date);

        try (CSVParser parser = new CSVParser(new FileReader(filePath), CSVFormat.DEFAULT.withHeader())) {
            if (!parser.getHeaderMap().containsKey(appConfig.getImsiColumn()) || !parser.getHeaderMap().containsKey(appConfig.getMsisdnColumn())) {
                logger.error("Missing headers in the CSV file for IMSI, MSISDN, or Activation Date.");
                return;
            }

            for (CSVRecord record : parser) {
                try {
                    String imsi = record.get(appConfig.getImsiColumn());
                    String msisdn = record.get(appConfig.getMsisdnColumn());

                    // Check if imsi or msisdn is empty
                    if (imsi.isEmpty() || msisdn.isEmpty()) {
                        logger.error("MSISDN or Imsi value is empty.");
                        return;
                    }

                    // Log record details
                    logger.info("Processing record: IMSI={}, MSISDN={}, Activation Date={}, Operator={}", imsi, msisdn, formattedDate, fixedOperatorValue);

                    // Add record to the database
                    dbOperations.addRecord(imsi, msisdn, formattedDate);

                    // Log success message
                    logger.info("Record processed successfully.");
                } catch (Exception e) {
                    // Log error message if there's an issue processing the record
                    logger.error("Error processing record: {}", e.getMessage(), e);
                    // Exit the loop if the query fails
                    return;
                }
            }
        }
    }
}
