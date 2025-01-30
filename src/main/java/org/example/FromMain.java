package org.example;

import org.example.utilities.Config;
import org.example.utilities.Log;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FromMain {

    Config config = Config.getInstance();
    private static final String LOG_TAG = "FromMain : ";
    private String INPUT_FILE_FULL_PATH;
    private String INPUT_FILE;
    private Map<String, CParseConf> conf;
    private Map<Integer, CHistoryFile> cdrHistoryPerComp;

    public FromMain() {
        Log.log("inside FromMain");
        conf = new HashMap<>();
        cdrHistoryPerComp = new HashMap<>();
        readDailyCdrConfig();
        readDailyCDRFileHistory();
        execProcess();
    }

    private boolean isSpecialCompany(String company) {
        return company.equals("013 - International Carrier for Subscriber") ||
                company.equals("018 - International Carrier for Subscriber") ||
                company.equals("012 - International Carrier for Subscriber") ||
                company.equals("015 - International Carrier for Subscriber") ||
                company.equals("0104 - International Carrier for Subscriber");
    }


    private String[] getFilesInDirectory(String directoryPath) {
        try {
            if (config.isDebug()) {
                // Get the base directory (the root of the project)
                String baseDir = System.getProperty("user.dir");

                // Define the new folder path relative to the project root
                directoryPath = baseDir + "/BLNG/Files/Input/Interface/DailyCdr/TZ/HOME_CELLULAR";
            }

            File directory = new File(directoryPath);
            if (!directory.exists()) {
                boolean created = directory.mkdirs(); // Create the full directory structure
                if (created) {
                    Log.log("Directory created successfully: " + directoryPath);
                } else {
                    Log.log("Failed to create directory: " + directoryPath);
                    return null;  // Return null if directory creation fails
                }
            }

            // Return the list of files in the directory
            return directory.list();
        } catch (Exception e) {
            Log.log("Error reading files from directory: " + directoryPath + " - " + e.getMessage());
            e.printStackTrace();  // Optionally log the stack trace for more details
            return null;  // Return null in case of error
        }
    }


    private boolean wasFileLoadedInHistory(CParseConf conf, String inputFile) {
        boolean fileLoaded = false;

        for (CHistoryFile history : cdrHistoryPerComp.values()) {
            if (history.getVenId() == conf.getId() && inputFile.equals(history.getFileName())) {
                fileLoaded = true;
                String newFileName = conf.getBackupFolder() + File.separator + inputFile;
                File newFile = new File(newFileName);
                File inputFileFullPath = new File(inputFile);

                if (newFile.exists()) {
                    boolean ans = newFile.delete();
                    if (!ans) Log.log("error at FromMain.java, couldn't delete existing file / folder");
                }

                boolean ans = inputFileFullPath.renameTo(newFile);
                if (!ans) Log.log("error at FromMain.java, couldn't rename existing file / folder");
                break;
            }
        }

        return fileLoaded;
    }

    private void readDailyCdrConfig() {
        Log.info("readDailyCdrConfig", this.getClass().getName());
        Database.fetchDailyCdrConfig(conf);
    }

    private void readDailyCDRFileHistory() {
        Log.log(LOG_TAG + "readDailyCDRFileHistory");
        Database.fetchDailyCDRFileHistory(cdrHistoryPerComp);
    }

    private void execProcess() {
        Log.log("execProcess");
        String[] filesInDirectory;
        int cdrFilesCounter = 0;
        int lastCDRFileID = cdrHistoryPerComp.size(); // Checks how many history files are there

        for (CParseConf currentConf : conf.values()) {
            Log.log("\n----------------------execProcess for company= " + currentConf.getTitle() + "-----------------------");

            if (isSpecialCompany(currentConf.getTitle())) {
                Log.log("Current writing file for company: " + currentConf.getTitle());
                CreateCDRMain createCDRMain = new CreateCDRMain(currentConf.getTitle(), currentConf);
                createCDRMain.process();
            }
            Log.log("ReadReturnedCDR");

            try {
                filesInDirectory = getFilesInDirectory(currentConf.getReadFolder());
                Log.log("INPUT_DIR: " + currentConf.getReadFolder() +
                        "\nOUTPUT_DIR: " + currentConf.getWriteFolder() +
                        "\nBACK_UP_DIR: " + currentConf.getBackupFolder());
                Log.log("Config details for company: " + currentConf.getTitle());
                for (String inputFileFullPath : filesInDirectory) {
                    String inputFile = Paths.get(inputFileFullPath).getFileName().toString();
                    Log.log("\ninputFile: " + inputFile);

                    if (!wasFileLoadedInHistory(currentConf, inputFile)) {
                        cdrFilesCounter++;
                        int currentCRDFileID = lastCDRFileID + cdrFilesCounter + 1;
                        Log.log("Current input file: " + inputFile);

                        ReadCDRMain readCDR = new ReadCDRMain(currentCRDFileID, inputFileFullPath, currentConf);
                        readCDR.process();
                    } else {
                        Log.log("inputFile: " + inputFile + " was already launched in history");
                    }
                }
            } catch (Exception e){
                Log.log(e);
            }
        }
        Log.log("Daily CDR process finished");
    }
}
