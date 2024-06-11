package org.example;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

@Configuration
public class AppConfig {

    @Value("${spring.datasource.url}")
    private String url;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${csv.imsi.column:imsi}")
    private String imsiColumn;

    @Value("${csv.msisdn.column:msisdn}")
    private String msisdnColumn;

    @Value("${csv.activation_date.column:activation_date}")
    private String activationDateColumn;

    @Value("${csv.operator.column:operator}")
    private String operatorColumn;

    public String getImsiColumn() {
        return imsiColumn;
    }

    public String getMsisdnColumn() {
        return msisdnColumn;
    }

    public String getActivationDateColumn() {
        return activationDateColumn;
    }

    public String getOperatorColumn() {
        return operatorColumn;
    }

    @Bean
    public DbOperations dbOperations() throws SQLException {
        return new DbOperations(url, username, password);
    }

    @Bean
    public AdditionModule additionModule(DbOperations dbOperations) {
        return new AdditionModule(dbOperations, this);
    }

    @Bean
    public DeletionModule deletionModule(DbOperations dbOperations) {
        return new DeletionModule(dbOperations, this);
    }
}
