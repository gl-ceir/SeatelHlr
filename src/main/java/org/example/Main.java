package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@SpringBootApplication
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    @Value("${spring.datasource.url}")
    private String url;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${start.date}")
    private String startDate;

    @Value("${end.date}")
    private String endDate;

    @Value("${add.folder.path}")
    private String addFolderPath;

    @Value("${del.folder.path}")
    private String delFolderPath;

    @Value("${add.file.pattern}")
    private String addFilePattern;

    @Value("${del.file.pattern}")
    private String delFilePattern;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Bean
    public CommandLineRunner run(DeletionModule deletionModule, AdditionModule additionModule) {
        return args -> {
            try {

                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                Date start = sdf.parse(startDate);
                Date end = sdf.parse(endDate);
                Calendar calendar = Calendar.getInstance();

                for (Date date = start; !date.after(end); calendar.setTime(date), calendar.add(Calendar.DATE, 1), date = calendar.getTime()) {
                    String formattedDate = sdf.format(date);
                    logger.info("Processing date: {}", formattedDate);

                    // Create filename filters based on patterns and date
                    FilenameFilter addFileFilter = (dir, name) -> name.matches(addFilePattern.replace("yyyyMMdd", formattedDate));
                    FilenameFilter delFileFilter = (dir, name) -> name.matches(delFilePattern.replace("yyyyMMdd", formattedDate));

                    // Find files matching the filters
                    File[] addFiles = new File(addFolderPath).listFiles(addFileFilter);
                    File[] delFiles = new File(delFolderPath).listFiles(delFileFilter);

                    if ((addFiles != null && addFiles.length > 1) || (delFiles != null && delFiles.length > 1)) {
                        logger.error("More than one file found for date {}", formattedDate);
                        return;
                    }

                    // Make the addFile and delFile so that they take the value at index 0
                    File addFile = (addFiles != null && addFiles.length > 0) ? addFiles[0] : null;
                    File delFile = (delFiles != null && delFiles.length > 0) ? delFiles[0] : null;

                    logger.info("Checking files for date: {}", formattedDate);

                    if (addFile != null && delFile == null) {
                        logger.error("Mismatched files for date {}", formattedDate);
                        return;
                    }

                    if (addFile == null && delFile != null) {
                        logger.error("Mismatched files for date {}", formattedDate);
                        return;
                    }

                    // Process files if both exist
                    if (addFile != null && delFile != null) {
                        logger.info("Processing deletion file: {}", delFile.getAbsolutePath());
                        deletionModule.processDeletionFile(delFile.getAbsolutePath());
                        logger.info("Processing addition file: {}", addFile.getAbsolutePath());
                        additionModule.processAdditionFile(addFile.getAbsolutePath(), date);
                    }
                }
            } catch (SQLException | IOException | java.text.ParseException e) {
                logger.error("An error occurred", e);
            }
        };
    }
}
