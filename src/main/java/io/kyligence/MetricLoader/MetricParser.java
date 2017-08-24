package io.kyligence.MetricLoader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetricParser {

    private static final Logger logger = LoggerFactory.getLogger(MetricParser.class);

    private String filePathName;
    private File file;

    private List<MetricRecord> metricList;

    public class MetricRecord {

        private String metricName;
        private long timestamp;
        private double value;
        private Map<String, String> tags;

        public String getMetricName() {
            return metricName;
        }

        public void setMetricName(String metricName) {
            this.metricName = metricName;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }

    }

    public MetricParser(String FilePathName) {

        this.filePathName = FilePathName;
        this.file = new File(this.filePathName);
        this.metricList = new ArrayList<MetricRecord>();

    }

    public void parse() {

        ObjectMapper objMapper = new ObjectMapper();

        try {

            JsonNode root = objMapper.readTree(file);

            // Counters, Histograms, Meters, Timers:
            Iterator<String> itr = root.fieldNames();

            while (itr.hasNext()) {

                String rootField = itr.next();

                JsonNode metricRoot = root.get(rootField);
                Iterator<String> itc = metricRoot.fieldNames();

                while (itc.hasNext()) {
                    String metricField = itc.next();
                    String[] metricDescs = metricField.split(":")[1].split(",");
                    String metricName = new String();
                    Map<String, String> tags = new HashMap<String, String>();
                    for (String metricDesc : metricDescs) {
                        String[] metricInfo = metricDesc.split("=");
                        if (metricInfo[0].equals("name")) {
                            metricName += metricInfo[1];
                        } else {
                            tags.put(metricInfo[0], metricInfo[1].replaceAll("\\[", ".").replaceAll("\\]", "")
                                    .replaceAll("<", "").replaceAll(">", "").replaceAll("-", "."));
                        }
                    }

                    JsonNode valueRoot = metricRoot.get(metricField);
                    Iterator<String> itv = valueRoot.fieldNames();
                    while (itv.hasNext()) {
                        String recordName = metricName;
                        String valueField = itv.next();
                        if (valueField.endsWith("units")) {
                            continue;
                        }
                        recordName += ("." + valueField);
                        int value = valueRoot.get(valueField).asInt();

                        MetricRecord record = new MetricRecord();
                        record.setMetricName(recordName);
                        record.setTimestamp(System.currentTimeMillis());
                        record.setValue(value);
                        record.setTags(tags);
                        logger.info("Parse success: " + record.getMetricName() + " = " + record.getValue() + " at "
                                + record.getTimestamp() + " with tag: " + record.getTags());
                        this.metricList.add(record);
                    }

                }

            }

        } catch (JsonProcessingException e) {
            logger.error("Error when parsing Json file: " + this.filePathName);
        } catch (IOException e) {
            logger.error("Error when reading file: " + this.filePathName);
        }

    }

    public List<MetricRecord> getMetricList() {
        return metricList;
    }

    // For test.
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
        MetricParser mp = new MetricParser(properties.getProperty("jsonPath"));
        mp.parse();
        System.out.println(mp.metricList.size());
    }

}
