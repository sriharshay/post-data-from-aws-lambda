package com.test.aws.lambda.feed;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.test.aws.lambda.feed.model.FeedModel;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * POST data
 */
public class PostData {
    private static final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private static final Logger logger = LoggerFactory.getLogger(PostData.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void handler(S3Event event) {
        event.getRecords().forEach(record -> {
            S3ObjectInputStream s3inputStream = s3
                    .getObject(record.getS3().getBucket().getName(), record.getS3().getObject().getKey())
                    .getObjectContent();
            try {
                logger.info("Reading data from s3 record");
                List<FeedModel> feed = Arrays
                        .asList(objectMapper.readValue(s3inputStream, FeedModel[].class));
                logger.info(feed.toString());
                s3inputStream.close();
                postData(feed);
            } catch (JsonMappingException e) {
                logger.error("JsonMappingException", e);
                throw new RuntimeException("Error while processing S3 event", e);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void postData(List<FeedModel> sampleDataList) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost("https://reqres.in/api/users");
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(sampleDataList);
        StringEntity entity = new StringEntity(json);
        logger.info("entity information [{}]", entity);
        logger.info("Data to be posted [{}]", json);
        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        CloseableHttpResponse response = client.execute(httpPost);
        logger.info("Status code [{}]", response.getStatusLine().getStatusCode());
        client.close();
    }

}