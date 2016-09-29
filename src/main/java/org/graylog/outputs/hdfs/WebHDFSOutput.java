package org.graylog.outputs.hdfs;

import com.google.inject.assistedinject.Assisted;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.http.client.AuthenticationType;
import org.apache.hadoop.fs.http.client.WebHDFSConnection;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.graylog2.plugin.streams.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebHDFSOutput implements MessageOutput {

    private static final Logger LOG = LoggerFactory.getLogger(WebHDFSOutput.class);

    private static final String CK_HDFS_HOST_NAME = "HDFS_HOST_NAME";
    private static final String CK_HDFS_PORT = "HDFS_PORT";
    private static final String CK_FILE = "FILE";
    private static final String CK_MESSAGE_FORMAT = "MESSAGE_FORMAT";
    private static final String CK_FLUSH_INTERVAL = "FLUSH_INTERVAL";
    private static final String CK_CLOSE_INTERVAL = "CLOSE_INTERVAL";
    private static final String CK_APPEND = "APPEND";
    private static final String CK_REOPEN = "REOPEN";
    private static final String CK_USERNAME = "USER_NAME";

    private static final String FIELD_SEPARATOR = " | ";
    private Configuration configuration;
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private String fileToWrite;
    private String messageFormat;
    private long flushIntervalInMillis;
    //private boolean append;
    private Timer flushTimer;
    private TimerTask flushTask;
    private WebHDFSConnection hdfsConnection;
    private List<MessageData> messagesToWrite;

    @Inject
    public WebHDFSOutput(@Assisted Stream stream, @Assisted Configuration configuration)
            throws MessageOutputConfigurationException, IOException {
        this.configuration = configuration;

        LOG.info("WebHDFSOutput launching...");

        String hostname = configuration.getString(CK_HDFS_HOST_NAME);
        int port = configuration.getInt(CK_HDFS_PORT);
        String username = configuration.getString(CK_USERNAME);

        hdfsConnection = new WebHDFSConnection("http://" + hostname + ":" + port, username, "anything",
                AuthenticationType.PSEUDO);

        messagesToWrite = new LinkedList<>();


        fileToWrite = configuration.getString(CK_FILE);
        if(fileToWrite.contains("%")) {
            fileToWrite = fileToWrite.replaceAll("%","%1\\$t");
        }
        messageFormat = configuration.getString(CK_MESSAGE_FORMAT);
        flushIntervalInMillis = configuration.getInt(CK_FLUSH_INTERVAL) * 1000;

        if(flushIntervalInMillis > 0) {
            flushTimer = new Timer("WebHDFS-Flush-Timer", true);
            flushTask = createFlushTask();
            flushTimer.schedule(flushTask, flushIntervalInMillis, flushIntervalInMillis);
        }

        //append = configuration.getBoolean(CK_APPEND);
        isRunning.set(true);
        LOG.info("WebHDFSOutput launched");
    }

    private TimerTask createFlushTask() {
        return new TimerTask() {
            @Override
            public void run() {
                try {
                    writeToHdfs();
                } catch (Exception e) {
                    LOG.warn("Exception while writing to HDFS", e);
                }
            }
        };
    }


    @Override
    public void stop() {
        LOG.info("Stopping WebHDFS output...");
        if(flushTask != null) {
            flushTask.cancel();
        }
        if(flushTimer != null) {
            flushTimer.cancel();
        }
        isRunning.set(false);
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    public void write(Message message) throws Exception {
        String path = getFormattedPath(message);
        String messageToWrite = getFormattedMessage(message);
        if (flushIntervalInMillis == 0) {
            writeToHdfs(path, messageToWrite);
        } else {
            synchronized (this) {
                messagesToWrite.add(new MessageData(path, messageToWrite));
            }
        }
    }

    private synchronized void writeToHdfs() throws IOException, AuthenticationException {
        Map<String, StringBuilder> pathToDataMap = new HashMap<>();
        for (MessageData message : messagesToWrite) {
            StringBuilder builder = pathToDataMap.get(message.getPath());

            if (builder == null) {
                builder = new StringBuilder();
                pathToDataMap.put(message.getPath(), builder);
            }
            builder.append(message.getMessage());
        }

        for (Map.Entry<String, StringBuilder> entry : pathToDataMap.entrySet()) {
            writeToHdfs(entry.getKey(), entry.getValue().toString());
        }
        messagesToWrite.clear();
    }

    private void writeToHdfs(String path, String data) throws IOException, AuthenticationException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(
                data.getBytes());
        try {
            hdfsConnection.append(path, inputStream);
        } catch (FileNotFoundException e) {
            hdfsConnection.create(path, inputStream);
        }
    }


    @Override
    public void write(List<Message> list) throws Exception {
        for (Message message : list) {
            write(message);
        }
    }


    private String getFormattedMessage(Message message) {
        String formattedMessage;
        if (messageFormat != null && messageFormat.length() > 0) {
            formattedMessage = StrSubstitutor.replace(messageFormat, message.getFields());
        } else {
            formattedMessage = String.valueOf(message.getTimestamp()) + FIELD_SEPARATOR +
                    message.getSource() + FIELD_SEPARATOR + message.getMessage();
        }

        if (!formattedMessage.endsWith("\n")) {
            formattedMessage = formattedMessage.concat("\n");
        }

        return formattedMessage;
    }

    private String getFormattedPath(Message message) {
        String formattedPath = fileToWrite;

        if (fileToWrite.contains("${")) {
            formattedPath = StrSubstitutor.replace(formattedPath, message.getFields());
        }

        if(fileToWrite.contains("%")) {
            formattedPath = String.format(formattedPath, message.getTimestamp().toDate());
        }

        return formattedPath;
    }

    public interface Factory extends MessageOutput.Factory<WebHDFSOutput> {
        @Override
        WebHDFSOutput create(Stream stream, Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest configurationRequest = new ConfigurationRequest();

            configurationRequest.addField(new TextField(
                            CK_HDFS_HOST_NAME,
                            "Host",
                            "",
                            "IP Address or hostname of HDFS server",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new NumberField(
                            CK_HDFS_PORT,
                            "Port",
                            50070,
                            "HDFS Web Port",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new TextField(
                            CK_USERNAME,
                            "Username",
                            "",
                            "User name for WebHDFS connection",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new TextField(
                            CK_FILE,
                            "File path",
                            "",
                            "Path of file to write messages." +
                                    "Accepts message fields like ${source} or date formats like %Y_%m_%d_%H_%M",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new TextField(
                            CK_MESSAGE_FORMAT,
                            "Message Format",
                            "${timestamp} | ${source} | ${message}",
                            "Format of the message to be written. Use message fields to format",
                            ConfigurationField.Optional.OPTIONAL)
            );

            configurationRequest.addField(new NumberField(
                            CK_FLUSH_INTERVAL,
                            "Flush Interval",
                            0,
                            "Flush interval in seconds. Recommended for high throughput outputs. 0 for immediate update",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            return configurationRequest;
        }
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("WebHDFS Output", false, "", "Forwards messages to HDFS for storage");
        }
    }

    private static class MessageData {
        private String path, message;

        public MessageData(String path, String messageToWrite) {
            this.path = path;
            this.message = messageToWrite;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
