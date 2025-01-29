package org.example.utilities;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static volatile Config instance;

    private final Properties properties;

    private Config() {
        properties = new Properties();
        loadProperties();
    }
    // Method to get the single instance of Config (singleton pattern)
    public static Config getInstance() {
        if (instance == null) {
            synchronized (Config.class) {
                if (instance == null) {
                    instance = new Config();
                }
            }
        }
        return instance;
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                Log.log("unable to find config.properties");
                return;
            }
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }


    public String getDbURL () { return properties.getProperty("db.url"); }
    public String getDbUser() { return properties.getProperty("db.user"); }
//    public String getDbPassword() { return System.getenv("DB_PASSWORD"); } // add this password to env and delete this comment before production: csr019
    public String getDbPassword() { return properties.getProperty("db.password"); } // add this password to env and delete this comment before production: csr019
    public String getLogLevel() { return properties.getProperty("log.level"); }
    public String getPrivateKey() {

        return properties.getProperty("privateKey");
    } //

    public boolean isDebug() {
        String str = properties.getProperty("isDebug");
        return str.equalsIgnoreCase("true");
    }
    public String getMailTo() {
        return properties.getProperty("mail.to");
    }
    public String getMailFrom() {
        return properties.getProperty("mail.from");
    }
    public String getHost() {
        return properties.getProperty("host");
    }
    public String getMailUsername() { return properties.getProperty("mail.username"); }
    public String getMailPassword() { return properties.getProperty("mail.password"); }
    public String getSMTPHost() { return properties.getProperty("smtp.host"); }
    public String getSMTPPort() { return properties.getProperty("smtp.port"); }
}
