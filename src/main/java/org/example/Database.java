package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.utilities.Config;
import org.example.utilities.Log;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Database {

    // HikariCP DataSource
    private static final HikariDataSource dataSource;

    // Static block to initialize HikariCP DataSource
    static {
        try {
            Config config = Config.getInstance();
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(config.getDbURL());
            hikariConfig.setUsername(config.getDbUser());
            hikariConfig.setPassword(config.getDbPassword());
            hikariConfig.setMaximumPoolSize(10); // Set pool size
            hikariConfig.setMinimumIdle(2);
            hikariConfig.setIdleTimeout(600000); // 10 minutes
            hikariConfig.setMaxLifetime(1800000); // 30 minutes
            hikariConfig.setConnectionTimeout(30000); // 30 seconds
            hikariConfig.setValidationTimeout(5000); // 5 seconds
            hikariConfig.setConnectionTestQuery("SELECT 1 FROM DUAL");
            hikariConfig.setDriverClassName("oracle.jdbc.OracleDriver");
            dataSource = new HikariDataSource(hikariConfig);
            Log.insertLogsToLogger("HikariCP DataSource initialized.", Database.class.getName());
        } catch (Exception e) {
            Log.insertLogsToLogger("Failed to initialize HikariCP DataSource " + e, Database.class.getName());
            throw new RuntimeException("Failed to initialize HikariCP DataSource", e);
        }
    }
    public static void testConnection() {
        try (Connection connection = dataSource.getConnection()) {
            if (connection != null && !connection.isClosed()) {
                Log.insertLogsToLogger("Connection is successful.", Database.class.getName());
            } else {
                Log.insertLogsToLogger("Connection failed or is closed.", Database.class.getName());
            }
        } catch (SQLException e) {
            Log.insertLogsToLogger("Connection test failed: " + e.getMessage(), Database.class.getName());
        }
    }

    // Private constructor to prevent instantiation
    private Database() {
        try (Connection connection = dataSource.getConnection()) {
            if (connection == null || connection.isClosed()) {
                throw new RuntimeException("Database connection could not be established.");
            }
            Log.insertLogsToLogger("Database connection established.", this.getClass().getName());
        } catch (SQLException e) {
            Log.insertLogsToLogger("Failed to establish database connection: " + e, Database.class.getName());
            throw new RuntimeException("Database connection failure", e); // Throw to prevent further execution
        }
    }

    public static void insertToTmpDaily(List<CFileElements.Element> elements) {
        final String INSERT_TO_TMP_DAILY =
                "INSERT INTO BLNG.tmp_daily_cdr (id, a_number, b_number, call_ansfer, duration, orig_row, isprepaid) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)";
        final int BATCH_SIZE = 1000; // Set the batch size

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_TO_TMP_DAILY)){
            connection.setAutoCommit(false);  // Start the transaction
            int batchCounter = 0;
            int i = 0;

            for (CFileElements.Element element : elements) {
                preparedStatement.setInt(1, ++i); // ID
                preparedStatement.setString(2, element.getANumber()); // A Number
                preparedStatement.setString(3, element.getBNumber()); // B Number
                preparedStatement.setTimestamp(4, new Timestamp(element.getCallDate().getTime())); // Call Answer
                preparedStatement.setInt(5, element.getDuration()); // Duration
                preparedStatement.setString(6, element.getOrigRow()); // Original Row
                preparedStatement.setString(7, element.getIsPrepaid()); // Is Prepaid

                preparedStatement.addBatch(); // Add to batch
                batchCounter++;

                // Execute batch if the counter reaches the batch size
                if (batchCounter == BATCH_SIZE) {
                    preparedStatement.executeBatch();
                    batchCounter = 0; // Reset the counter
                }
            }

            // Execute the remaining batch
            if (batchCounter > 0) {
                preparedStatement.executeBatch();
            }

            // Commit the transaction if everything is successful
            connection.commit();

        } catch (SQLException e) {
            // Log the error and rollback the transaction if an exception occurs
            Log.insertLogsToLogger("Failed to insert data to tmp_daily_cdr table: " + e.getMessage(), Database.class.getName());
        }
    }



    public static void startPackageProcess(CParseConf config, int fileId) {
        Log.insertLogsToLogger("StartPackageProcess ", Database.class.getName());
        final String PROCEDURE_NAME = "BLNG.PKG_daily_cdr_tz.process";

        try (Connection connection = dataSource.getConnection();
             CallableStatement callableStatement = connection.prepareCall("{call " + PROCEDURE_NAME + "(?, ?, ?)}")) {

            // Set parameters for the stored procedure
            callableStatement.setInt(1, config.getId()); // p_conf
            callableStatement.setInt(2, fileId);        // p_file_id
            callableStatement.setInt(3, config.getId()); // p_vendor_id

            callableStatement.execute(); // Execute the stored procedure

        } catch (SQLException e) {
            Log.insertLogsToLogger("Failed to execute procedure " + PROCEDURE_NAME + ": " + e.getMessage(), Database.class.getName());
        }
    }

    public static void insertToDailyCdrErrorFiles(String fileName, int confId, int totalPricingRowsInFile, int errorRows, String isHeaderInRightFormat, String isTrailerInRightFormat) {
        String query = "insert into BLNG.daily_cdr_error_files (id, file_name, vendor_id, loaded_date, readed_rows, error_rows, header_ok, trailer_ok) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        int id = 1;
        Date date = new Date();
        date.getTime();
        try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            connection.setAutoCommit(false);

            preparedStatement.setInt(1, id);
            preparedStatement.setString(2, fileName);
            preparedStatement.setInt(3, confId);
            preparedStatement.setTimestamp(4, new Timestamp(date.getTime()));  // Set the timestamp
            preparedStatement.setInt(5, totalPricingRowsInFile);
            preparedStatement.setInt(6, errorRows);
            preparedStatement.setString(7, isHeaderInRightFormat);
            preparedStatement.setString(8, isTrailerInRightFormat);

            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            Log.insertLogsToLogger("error", e, Database.class.getName());
        }
    }

    // Method to execute a query and return the ResultSet
    public static ResultSet executeQuery(String query) {
        ResultSet resultSet = null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            Log.insertLogsToLogger("error", e, Database.class.getName());
        }
        return resultSet;
    }

    // Static inner class for Singleton implementation (thread-safe)
    private static class DatabaseSingleton {
        private static final Database INSTANCE = new Database();
    }

    // Public method to get the singleton instance
    public static Database getInstance() {
        return DatabaseSingleton.INSTANCE;
    }

    public static void initialize() {
        getInstance(); // This ensures the database is initialized
    }

//    public static void insertLogsToDatabase(String level, String message, String className) {
//        final String INSERT_TO_LOGS =
//                "INSERT INTO Blng.log_main (load_date, type, INDETIFIER_NUM, status, info_txt_1) VALUES (?, ?, ?, ?, ?)";
//        try (Connection connection = dataSource.getConnection();
//             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_TO_LOGS)) {
//
//            // Set parameters for the prepared statement
//            preparedStatement.setDate(1, new java.sql.Date(loadDate.getTime())); // Oracle expects java.sql.Date for DATE
//            preparedStatement.setInt(2, type); // Set the type (number)
//            preparedStatement.setInt(3, fileSeq); // Set the file sequence (number)
//            preparedStatement.setString(4, status); // Set the status (varchar)
//            preparedStatement.setString(5, ensureNotOutOfRange(message, 4000)); // Truncate message if necessary
//
//            preparedStatement.executeUpdate(); // Execute the insert statement
//        } catch (SQLException e) {
//            Log.insertLogsToLogger("Failed to insert log to database: " + e, Database.class.getName());
//        }
//    }

//    public static void insertLogsToDatabase(java.util.Date loadDate, int type, int fileSeq, String status, String message) {
//        final String INSERT_TO_LOGS =
//                "INSERT INTO Blng.log_main (load_date, type, INDETIFIER_NUM, status, info_txt_1) VALUES (?, ?, ?, ?, ?)";
//        try (Connection connection = dataSource.getConnection();
//             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_TO_LOGS)) {
//
//            // Set parameters for the prepared statement
//            preparedStatement.setDate(1, new java.sql.Date(loadDate.getTime())); // Oracle expects java.sql.Date for DATE
//            preparedStatement.setInt(2, type); // Set the type (number)
//            preparedStatement.setInt(3, fileSeq); // Set the file sequence (number)
//            preparedStatement.setString(4, status); // Set the status (varchar)
//            preparedStatement.setString(5, ensureNotOutOfRange(message, 4000)); // Truncate message if necessary
//
//            preparedStatement.executeUpdate(); // Execute the insert statement
//        } catch (SQLException e) {
//            Log.insertLogsToLogger("Failed to insert log to database: " + e, Database.class.getName());
//        }
//    }

    public static void fetchDailyCdrConfig(Map<String, CParseConf> configMap) {
        final String FETCH_CDR_CONFIG_QUERY =
                "SELECT id, title, input_row_format, output_row_format, date_format, " +
                        "prepaid_value, vendor_id, prepaid_vendor_id, read_folder, write_folder, " +
                        "backup_folder, header_format, trailer_format, last_seq, error_folder " +
                        "FROM BLNG.daily_cdr_config " +
                        "WHERE our_company = 'TZ' AND sysdate BETWEEN effective_date_from AND NVL(effective_date_to, sysdate)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(FETCH_CDR_CONFIG_QUERY);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String title = resultSet.getString("title");
                CParseConf conf = new CParseConf(resultSet);
                configMap.put(title, conf);
            }
        } catch (SQLException e) {
            Log.insertLogsToLogger("error", e, Database.class.getName());
        }
    }

    public static void fetchDailyCDRFileHistory(Map<Integer, CHistoryFile> cdrHistoryPerComp) {
        final String FETCH_CDR_FILE_HISTORY = "SELECT * from BLNG.daily_cdr_file_history";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(FETCH_CDR_FILE_HISTORY);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            int i = 0;
            while (resultSet.next()) {
                CHistoryFile cHistoryFile = new CHistoryFile(resultSet);
                cdrHistoryPerComp.put(i, cHistoryFile);
                i++;
            }
        } catch (SQLException e) {
            Log.insertLogsToLogger("error", e, Database.class.getName());
        }
    }

    // Method to fetch data from the tmp_daily_cdr table
    public static ResultSet readBackFromTmpDailyCDR() throws SQLException {
        String query = "SELECT a_number, b_number, call_ansfer, duration, price, isprepaid, ispostpaid FROM BLNG.tmp_daily_cdr ORDER BY id";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            return preparedStatement.executeQuery();
        }
    }
    public static void insertToDailyCdrHistoryFile(String fileName, CParseConf conf, int len, int totalPricingRowsInFile, int errorRows, int fileId) {
        Log.insertLogsToLogger("info", "insertToDailyCdrHistoryFile", Database.class.getName());

        int id = 1;
        LocalDateTime dateTimeNow = LocalDateTime.now();

        String query = "INSERT INTO BLNG.daily_cdr_file_history (id, file_name, vendor_id, loaded_date, loaded_rows, reated_rows, error_rows, file_seq) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {
            connection.setAutoCommit(false);
            stmt.setInt(1, id);
            stmt.setString(2, fileName);
            stmt.setInt(3, conf.getId());
            stmt.setObject(4, Timestamp.valueOf(dateTimeNow)); // Convert LocalDateTime to Timestamp
            stmt.setInt(5, len);
            stmt.setInt(6, totalPricingRowsInFile);
            stmt.setInt(7, errorRows);
            stmt.setInt(8, fileId);

            stmt.executeUpdate();

            // Commit the transaction if everything is successful
            connection.commit();
        } catch (SQLException e) {
            Log.insertLogsToLogger("error", e, Database.class.getName());
        }
    }

    public static void insertToDailyCdrStatistics(Map<Date, DailyCdrStatistics> statisticsForEachDay) {
        final String INSERT_TO_DAILY_CDR_STATISTIC =
                "INSERT INTO BLNG.daily_cdr_statistic " +
                        "(id, file_id, vendor_id, cdr_date, prepaid_duration, prepaid_cdr_row, prepaid_charge, " +
                        "postpaid_duration, postpaid_cdr_row, occ_duration, occ_cdr_row, occ_charge) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        final int BATCH_SIZE = 1000; // Set the batch size

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_TO_DAILY_CDR_STATISTIC)) {
            connection.setAutoCommit(false);
            int batchCounter = 0;
            int id = 0;

            for (Map.Entry<Date, DailyCdrStatistics> entry : statisticsForEachDay.entrySet()) {
                Date date = entry.getKey();
                DailyCdrStatistics stat = entry.getValue();

                preparedStatement.setInt(1, ++id); // ID
                preparedStatement.setInt(2, stat.getFileId()); // File ID
                preparedStatement.setInt(3, stat.getVendorId()); // Vendor ID
                preparedStatement.setDate(4, new java.sql.Date(date.getTime())); // CDR Date
                preparedStatement.setInt(5, stat.getPrepaidSumDuration()); // Prepaid Duration
                preparedStatement.setInt(6, stat.getPrepaidRowCount()); // Prepaid CDR Row
                preparedStatement.setBigDecimal(7, stat.getPrepaidSumCharge()); // Prepaid Charge
                preparedStatement.setInt(8, stat.getPostpaidSumDuration()); // Postpaid Duration
                preparedStatement.setInt(9, stat.getPostpaidRowCount()); // Postpaid CDR Row
                preparedStatement.setInt(10, stat.getOccSumDuration()); // OCC Duration
                preparedStatement.setInt(11, stat.getOccRowCount()); // OCC CDR Row
                preparedStatement.setBigDecimal(12, stat.getOccSumCharge()); // OCC Charge

                preparedStatement.addBatch();
                batchCounter++;

                // Execute batch when the batch size is reached
                if (batchCounter == BATCH_SIZE) {
                    preparedStatement.executeBatch();
                    batchCounter = 0; // Reset the counter
                }
            }

            // Execute any remaining records in the batch
            if (batchCounter > 0) {
                preparedStatement.executeBatch();
            }

            // Handle empty map case
            if (statisticsForEachDay.isEmpty()) {
                preparedStatement.setInt(1, 0); // ID
                preparedStatement.setInt(2, 0); // File ID
                preparedStatement.setInt(3, 0); // Vendor ID
                preparedStatement.setDate(4, new java.sql.Date(System.currentTimeMillis())); // CDR Date
                preparedStatement.setInt(5, 0); // Prepaid Duration
                preparedStatement.setInt(6, 0); // Prepaid CDR Row
                preparedStatement.setBigDecimal(7, BigDecimal.ZERO); // Prepaid Charge
                preparedStatement.setInt(8, 0); // Postpaid Duration
                preparedStatement.setInt(9, 0); // Postpaid CDR Row
                preparedStatement.setInt(10, 0); // OCC Duration
                preparedStatement.setInt(11, 0); // OCC CDR Row
                preparedStatement.setBigDecimal(12, BigDecimal.ZERO); // OCC Charge

                preparedStatement.executeUpdate(); // Execute for empty case

                // Commit the transaction if everything is successful
                connection.commit();
            }
        } catch (SQLException e) {
            Log.insertLogsToLogger("error", e, Database.class.getName());
        }
    }

    public static ResultSet readRowsFromDB(String company) throws SQLException {
        String query;
        if (company.equals("HOT")) {
            query = "SELECT c.a_num, CASE WHEN LENGTH(c.redirecting_number) > 7 THEN c.redirecting_number ELSE NULL END AS redirecting_number, " +
                    "c.b_num, c.call_answer, c.call_release, c.call_duration, cr.rate " +
                    "FROM BLNG.cdr_rates cr JOIN BLNG.cdr c ON cr.cdr_id = c.id AND cr.call_release = c.call_release " +
                    "AND cr.rate_type = 7 AND cr.call_release >= TRUNC(SYSDATE) - 1 AND cr.call_release < TRUNC(SYSDATE) " +
                    "JOIN vendor v ON c.owner_in_id = v.id AND v.id = ? " +
                    "WHERE c.owner_out_id IN (SELECT id FROM vendor WHERE vendor_type = 3) " +
                    "AND c.a_num NOT IN (SELECT DISTINCT TO_CHAR(phone) FROM subscribers rs JOIN customer cu " +
                    "ON cu.customer_id = rs.customer_id AND cu.effective_date_to IS NULL WHERE rs.status = 1) " +
                    "AND LENGTH(c.a_num) > 7 AND LENGTH(c.b_num) > 9 AND c.a_num NOT LIKE '5%' ORDER BY c.id";
        } else {
            query = "SELECT c.a_num, c.redirecting_number, c.b_num, c.call_answer, c.call_release, c.call_duration, cr.rate " +
                    "FROM BLNG.cdr_rates cr JOIN BLNG.cdr c ON cr.cdr_id = c.id AND cr.call_release = c.call_release " +
                    "AND cr.rate_type = 7 AND cr.call_release >= TRUNC(SYSDATE) - 1 AND cr.call_release < TRUNC(SYSDATE) " +
                    "JOIN vendor v ON c.owner_in_id = v.id AND v.id = ? " +
                    "WHERE c.owner_out_id IN (SELECT id FROM vendor WHERE vendor_type = 3) " +
                    "AND LENGTH(c.a_num) > 7 AND LENGTH(c.b_num) > 9 ORDER BY c.id";
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, company);
            return ps.executeQuery();
        }
    }
}



