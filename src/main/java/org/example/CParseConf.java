package org.example;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CParseConf {
    private final int id;
    private final String title;
    private final String inputRowFormat;
    private final String outputRowFormat;
    private final String dateFormat;
    private final String prepaidValue;
    private final int venId;
    private final int prepaidVendorId;
    private final String readFolder;
    private final String writeFolder;
    private final String backupFolder;
    private final String errorFolder;
    private final String headerFormat;
    private final String trailerFormat;
    private final int lastSeq;

    public CParseConf(ResultSet resultSet) throws SQLException {


        // Assign values based on the result set columns
        this.id = resultSet.getInt(1); // id (column 1 in SQL is index 0 in C#)
        this.title = resultSet.getString(2); // title
        this.inputRowFormat = resultSet.getString(3); // input_row_format
        this.outputRowFormat = resultSet.getString(4); // output_row_format
        this.dateFormat = resultSet.getString(5); // date_format
        this.prepaidValue = resultSet.getString(6); // prepaid_value (nullable)
        this.venId = resultSet.getObject(7) != null ? resultSet.getInt(7) : 0; // vendor_id (nullable)
        this.prepaidVendorId = resultSet.getObject(8) != null ? resultSet.getInt(8) : 0; // prepaid_vendor_id (nullable)
        this.readFolder = resultSet.getString(9); // read_folder
        this.writeFolder = resultSet.getString(10); // write_folder
        this.backupFolder = resultSet.getString(11); // backup_folder
        this.headerFormat = resultSet.getString(12); // header_format
        this.trailerFormat = resultSet.getObject(13) != null ? resultSet.getString(13) : ""; // trailer_format (nullable)
        this.lastSeq = resultSet.getObject(14) != null ? resultSet.getInt(14) : 0; // last_seq (nullable)
        this.errorFolder = resultSet.getString(15);
    }

    public int getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getInputRowFormat() {
        return inputRowFormat;
    }

    public String getOutputRowFormat() {
        return outputRowFormat;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String getPrepaidValue() {
        return prepaidValue;
    }

    public int getVenId() {
        return venId;
    }

    public int getPrepaidVendorId() {
        return prepaidVendorId;
    }

    public String getReadFolder() {
        return readFolder;
    }

    public String getWriteFolder() {
        return writeFolder;
    }

    public String getBackupFolder() {
        return backupFolder;
    }

    public String getErrorFolder() {
        return errorFolder;
    }

    public String getHeaderFormat() {
        return headerFormat;
    }

    public String getTrailerFormat() {
        return trailerFormat;
    }

    public int getLastSeq() {
        return lastSeq;
    }

    @Override
    public String toString() {
        return String.format(
                "ID:[%d], Title:[%s], InputRowFormat:[%s], OutputRowFormat:[%s], DateFormat:[%s], PrepaidValue:[%s]",
                id, title, inputRowFormat, outputRowFormat, dateFormat, prepaidValue
        );
    }
}
