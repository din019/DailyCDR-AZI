package org.example;

import org.example.ReadCDRElements;
import org.example.utilities.Log;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.stream.Stream;

public class ReadCDRMain implements AutoCloseable {
    private static final String LOG_TAG = "ReadCDRMain: ";
    private String fileName;
    private int fileID;
    private ReadCDRElements readCDRElements;
    private boolean isHeaderInRightFormat = false;
    private boolean isTrailerInRightFormat = false;
    private boolean isFileFormatGood = false;
    private int totalRowsInFile = 0;
    private int totalCallRowsInFile = 0;
    private int wrongFormatRowsCount = 0;
    private boolean wasThereBadFormattedRow = false;
    private int currentLineNumber = 0;
    private String header = "";
    private String trailer = "";
    CParseConf currentConf;

    public ReadCDRMain(int fileID, String fileName, CParseConf currentConf) {
        this.fileID = fileID;
        this.fileName = fileName;
        this.readCDRElements = new ReadCDRElements(fileID, new File(fileName).getName());
        this.currentConf = currentConf;
    }

    public void process() {
        System.out.println("Starting reading input file: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));

        readFile();

        if (isFileFormatGood) {
            moveFile(new File(fileName).getParent(), "backup_folder", new File(fileName).getName());
        } else {
            moveFile(new File(fileName).getParent(), "error_folder", new File(fileName).getName());
        }
    }

    private void readFile() {
        try (Stream<String> lines = Files.lines(Paths.get(fileName))) {
            totalRowsInFile = (int) lines.count();
        } catch (IOException e) {
            Log.log("Error reading file: " + e.getMessage());
        }


        try (BufferedReader fileReader = new BufferedReader(new FileReader(fileName))) {
            Pattern pattern = Pattern.compile(currentConf.getInputRowFormat(), Pattern.CASE_INSENSITIVE);
            header = fileReader.readLine();
            isHeaderInRightFormat = header.matches(currentConf.getHeaderFormat());

            if (!isHeaderInRightFormat) {
                Log.error( "Header is in bad format",this.getClass().getName());
            }
            currentLineNumber++;

            String line;
            while ((line = fileReader.readLine()) != null && isHeaderInRightFormat) {
                currentLineNumber++;
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    totalCallRowsInFile++;
                    readCDRElements.add(line, matcher);
                } else {
                    if (!insideTrailerLine()) {
                        Log.log(LOG_TAG + "Error: Found bad format line at " + currentLineNumber, "ERROR");
                        wrongFormatRowsCount++;
                        wasThereBadFormattedRow = true;
                        break;
                    } else {
                        trailer = line;
                    }
                }
            }

            isTrailerInRightFormat = trailer.matches(Database.getTrailerFormat());
            isFileFormatGood = isHeaderInRightFormat && isTrailerInRightFormat && !wasThereBadFormattedRow;

            if (isFileFormatGood && totalCallRowsInFile > 0) {
                readCDRElements.doInsert();
            }

        } catch (IOException e) {
            Log.log(LOG_TAG + e.getMessage(), "ERROR");
        }
    }

    private boolean insideTrailerLine() {
        return currentLineNumber > totalRowsInFile - 1;
    }

    private void moveFile(String currentPath, String destinationPath, String fileName) {
        File source = new File(currentPath, fileName);
        File destination = new File(destinationPath, fileName);
        if (destination.exists()) {
            destination.delete();
        }
        boolean success = source.renameTo(destination);
        if (success) {
            Log.log(fileName + " moved to: " + destinationPath, "INFO");
        } else {
            Log.log(LOG_TAG + "Error moving file", "ERROR");
        }
    }

    @Override
    public void close() {
        Log.log(LOG_TAG + "Cleaning up resources", "INFO");
    }
}
