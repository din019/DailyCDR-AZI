package org.example.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log {
    public final static String logLevel; // if this is not final it will not be thread safe

    // Static initialization of logLevel
    static {
        Config config = Config.getInstance();
        logLevel = config.getLogLevel();
    }

    public static void log(String level, String message, String className){
        className = className.replaceAll("org.example.","");
        if (shouldLog(level)) {
//            Database.insertLogsToDatabase(level, message, className);
        }
        insertLogsToLogger(level, message, className);
    }

    public static void log(String message) {
        insertLogsToLogger("info", message, Log.class.getName());
    }
    public static void log(Exception e) {
        String errorMessage = getStackTrace(e);
        insertLogsToLogger("info", errorMessage, Log.class.getName());
    }
    public static void info(String message, String className){
        log("info", message, className);
    }
    public static void error(String message, String className){
        log("error", message, className);
    }
    public static void error(String message, Exception e, String className) {
        String errorMessage = message + " " + e.getMessage() + "\n" + getStackTrace(e);
        error(errorMessage, className);
    }
    public static void debug(String message, String className){
        log("debug", message, className);
    }
    public static void warn(String message, String className){
        log("warn", message, className);
    }
    public static void insertLogsToLogger(String level, String message, String className){
        Logger LOGGER = LoggerFactory.getLogger(className);
        logWithLogger(level, message, LOGGER);
    }
    public static void insertLogsToLogger(String level, Exception e, String className){
        String errorMessage = getStackTrace(e);
        insertLogsToLogger(level, errorMessage, className);
    }
    public static void insertLogsToLogger(String message, String className){
        insertLogsToLogger("info", message, className);
    }

    public static void insertLogsToLogger(String level, String message, Exception e, String className){
        String errorMessage = message + " " + e.getMessage() + "\n" + getStackTrace(e);
        insertLogsToLogger(level, errorMessage, className);
    }
    private static void logWithLogger(String level, String message, Logger LOGGER) {
        if (level.equalsIgnoreCase("info")) {
            LOGGER.info(message);
        } else if (level.equalsIgnoreCase("debug")) {
            LOGGER.debug(message);
        } else if (level.equalsIgnoreCase("warn")) {
            LOGGER.warn(message);
        } else if (level.equalsIgnoreCase("error")) {
            LOGGER.error(message);
        }
    }

    // Helper method to get the stack trace as a string
    private static String getStackTrace(Exception e) {
        StringBuilder stackTrace = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            stackTrace.append(element.toString()).append("\n");
        }
        return stackTrace.toString();
    }

    private static boolean shouldLog(String level) {
        // Determine if the message should be logged based on logLevel
        if (level == null)
            return true;
        switch (logLevel.toLowerCase()) {
            case "debug":
                return true; // Log everything
            case "info":
                return !level.equalsIgnoreCase("debug");
            case "warn":
                return level.equalsIgnoreCase("warn") || level.equalsIgnoreCase("error");
            case "error":
                return level.equalsIgnoreCase("error");
            default:
                return false; // If no match, log nothing
        }
    }
}
