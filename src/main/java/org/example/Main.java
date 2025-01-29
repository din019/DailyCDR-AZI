package org.example;

import org.example.utilities.Config;
import org.example.utilities.Log;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

import static org.example.Database.testConnection;

public class Main {
    public static void main(String[] args) {
        try {
            Database.getInstance();
            testConnection();
            new FromMain();
        } catch (Exception e) {
            Config config = Config.getInstance();
            Log.log(e.getMessage());
            String mailTo = config.getMailTo();  // Recipient email
            String apiKey = config.getPrivateKey();   // API key or password for SMTP authentication

            // Set the properties for the SMTP client
            Properties properties = new Properties();
            properties.put("mail.smtp.host", config.getSMTPHost());
            properties.put("mail.smtp.port", config.getSMTPPort());
            properties.put("mail.smtp.auth", "true");
            properties.put("mail.smtp.starttls.enable", "true");  // Enable TLS if needed

            // Set up the session with the SMTP server
            Session session = Session.getInstance(properties, new javax.mail.Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication("apikey", apiKey);  // SMTP username and API key
                }
            });

            try {
                // Create a new email message
                Message message = new MimeMessage(session);
                message.setFrom(new InternetAddress(config.getMailFrom()));  // Sender email
                message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mailTo));  // Recipient email
                message.setSubject("TZ mabal error");  // Subject of the email
                message.setContent("התרחשה שגיאה בהרצת קבצים שלנו כמפעיל בנלאומי  <br /><br />" + "Error details here", "text/html");  // Body content (HTML)

                // Send the email
                Transport.send(message);

                Log.log("Email was sent to " + mailTo);

            } catch (Exception e2) {
                Log.log(e2);
            }
        }
    }
}