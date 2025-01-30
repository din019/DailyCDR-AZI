package org.example;

import org.example.utilities.Log;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class CHistoryFile {
    private int id;
    private String fileName;
    private int venId;
    private java.sql.Timestamp loadedDate;
    private int loadedRows;
    private int createdRows;
    private int errorRows;

    public int getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }

    public int getVenId() {
        return venId;
    }

    public java.sql.Timestamp getLoadedDate() {
        return loadedDate;
    }

    public int getLoadedRows() {
        return loadedRows;
    }

    public int getCreatedRows() {
        return createdRows;
    }

    public int getErrorRows() {
        return errorRows;
    }


    public CHistoryFile(ResultSet resultSet) throws SQLException {
//        Log.log("info", "CHistoryFile: Constructor", this.getClass().getName());

        this.id = resultSet.getInt(1);
        this.fileName = resultSet.getString(2);
        this.venId = resultSet.getInt(3);
        this.loadedDate = resultSet.getTimestamp(4);

    }
}
