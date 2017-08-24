package io.kyligence.MetricLoader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.MetricLoader.MetricParser.MetricRecord;

public class MetricWatcher {

    private static final Logger logger = LoggerFactory.getLogger(MetricWatcher.class);

    private String filePathName;

    public MetricWatcher(String filePathName) {
        this.filePathName = filePathName;
    }

    public String getFilePathName() {
        return filePathName;
    }

    public void setFilePathName(String filePathName) {
        this.filePathName = filePathName;
    }

    public static void main(String[] args) {
        
        String confPath = "conf/conf.properties";
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(confPath));
        } catch (FileNotFoundException e) {
            logger.error("File " + confPath + " not found! ");
        } catch (IOException e) {
            logger.error("Error when reading file: " + confPath);
        }

        File file = new File(properties.getProperty("jsonPath"));
        String watchPath = file.getParent();
        MetricWatcher metricWatcher = new MetricWatcher(watchPath);

        Path path = Paths.get(metricWatcher.getFilePathName());

        while (true) {
        
            try {
                WatchService watcher = path.getFileSystem().newWatchService();
                path.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
                WatchKey watchKey = watcher.take();

                List<WatchEvent<?>> events = watchKey.pollEvents();
                for (WatchEvent<?> event : events) {
                    if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE || event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                        MetricParser metricParser = new MetricParser(properties.getProperty("jsonPath"));
                        metricParser.parse();
                        MetricWriter metricWriter = new MetricWriter(properties.getProperty("host"), properties.getProperty("port"));
                        for (MetricRecord record : metricParser.getMetricList()) {
                            boolean result = metricWriter.writeRecord(record);
                            if (!result) {
                                logger.error("Error when writing record: " + record.getMetricName());
                            }
                        }
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

}
