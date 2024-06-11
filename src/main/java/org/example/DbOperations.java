package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DbOperations {
    private static final Logger logger = LoggerFactory.getLogger(DbOperations.class);
    private Connection connection;

    @Value("${fixed.operator.value}")
    private String fixedOperatorValue;

    public DbOperations(String url, String username, String password) throws SQLException {
        try {
            this.connection = DriverManager.getConnection(url, username, password);
            logger.info("Connected to database.");
        } catch (SQLException e) {
            logger.error("Error connecting to database: {}", e.getMessage());
            throw e; // Rethrow the exception to propagate it
        }
    }

    public void deleteRecord(String msisdn) throws SQLException {
        String query = "DELETE FROM active_msisdn_list WHERE msisdn = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, msisdn);
            int rowsAffected = statement.executeUpdate();
            logger.info("{} record(s) deleted from active_msisdn_list for MSISDN: {}", rowsAffected, msisdn);
        } catch (SQLException e) {
            logger.error("Error deleting record: {}", e.getMessage());
            throw e; // Rethrow the exception to propagate it
        }
    }

    public void addRecord(String imsi, String msisdn, String formattedDate) throws SQLException {
        String query = "INSERT INTO active_msisdn_list (imsi, msisdn, activation_date, operator) VALUES (?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, imsi);
            statement.setString(2, msisdn);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            java.util.Date utilDate = sdf.parse(formattedDate);

            // Convert java.util.Date to java.sql.Date
            java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());

            statement.setDate(3, sqlDate);
            statement.setString(4, fixedOperatorValue);
            int rowsAffected = statement.executeUpdate();
            logger.info("{} record(s) added to active_msisdn_list for MSISDN: {}", rowsAffected, msisdn);
        } catch (SQLException e) {
            logger.error("Error adding record: {}", e.getMessage());
            throw e; // Rethrow the exception to propagate it
        } catch (ParseException e) {
            logger.error("Error parsing date: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
