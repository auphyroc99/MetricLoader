package io.kyligence.MetricLoader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kyligence.MetricLoader.MetricParser.MetricRecord;

public class MetricWriter {
    
    private static final Logger logger = LoggerFactory.getLogger(MetricWriter.class);
    
    private String host;
    private String port;
    private String baseURL;
    
    public MetricWriter(String host, String port) {
        setHost(host);
        setPort(port);
        init();
    }
    
    private void init() {
        this.baseURL = "http://" + this.host + ":" + this.port + "/api/put";
        logger.info("Init baseURL in MetricWriter as: " + this.baseURL);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getBaseURL() {
        return baseURL;
    }

    public void setBaseURL(String baseURL) {
        this.baseURL = baseURL;
    }
    
    public boolean writeRecord(MetricRecord record) {
        
        HttpPost post = new HttpPost(baseURL);
                
        Map<String, Object> recordMap = new HashMap<String, Object>();
        recordMap.put("metric", record.getMetricName());
        recordMap.put("timestamp", record.getTimestamp());
        recordMap.put("value", record.getValue());
        recordMap.put("tags", record.getTags());

        try {
            String postMsg = new ObjectMapper().writeValueAsString(recordMap);
            logger.info("Sending record: " + postMsg);
            
            post.setEntity(new StringEntity(postMsg, "UTF-8"));
            
            HttpResponse response = new DefaultHttpClient().execute(post);
            
            if (response.getStatusLine().getStatusCode() != 204) {
                logger.info("Response error code: " + response.getStatusLine().getStatusCode());
                return false;
            }
            
            EntityUtils.consume(response.getEntity());
            
        } catch (Exception e) {
            return false;
        }
        
        return true;        

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
        MetricWriter mw = new MetricWriter(properties.getProperty("host"), properties.getProperty("port"));
        for (MetricRecord record : mp.getMetricList()) {
            boolean result = mw.writeRecord(record);
            if (! result) {
                logger.error("Error when writing record: " + record.getMetricName());
            }
        }        
    }
}
