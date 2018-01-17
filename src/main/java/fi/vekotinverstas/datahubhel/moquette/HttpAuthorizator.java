package fi.vekotinverstas.datahubhel.moquette;

import io.moquette.spi.security.IAuthorizator;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HttpAuthorizator implements IAuthorizator {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAuthorizator.class);
    private static final int SUB = 1;
    private static final int PUB = 2;

    private String aclUri = null;
    private Cache<String, Boolean> cache;

    public HttpAuthorizator() {
        this.cache = new Cache2kBuilder<String, Boolean>() {}
                .name("MoquetteHttpAuthorizatorCache")
                .entryCapacity(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();

        LOGGER.info("Getting MoquetteHttpAuthorizatorUri from context");
        try {
            this.aclUri = (String) new InitialContext().lookup("java:comp/env/MoquetteHttpAuthorizatorUri");
            LOGGER.info("ACL URI: " + this.aclUri);
        } catch (NamingException e) {
            LOGGER.warn("Couldn't find MoquetteHttpAuthorizatorUri setting from context");
        }
    }

    private String getCacheKey(String topic, String user, String client, Integer type) {
        return topic + '-' + user + '-' + client + '-' + type.toString();
    }

    private boolean canDo(String topic, String user, String client, Integer type) {
        if (this.aclUri == null) {
            LOGGER.warn("MoquetteHttpAuthorizatorUri not set. Denying access.");
            return false;
        }

        String cacheKey = this.getCacheKey(topic, user, client, type);

        Boolean result = this.cache.peek(cacheKey);
        if (result != null) {
            LOGGER.info("Found in cache: " + result.toString());
            return result;
        }
        LOGGER.info("Not found in cache");
        result = false;

        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(this.aclUri);

        List<NameValuePair> arguments = new ArrayList<>(4);
        arguments.add(new BasicNameValuePair("username", user));
        arguments.add(new BasicNameValuePair("clientid", client));
        arguments.add(new BasicNameValuePair("topic", topic));
        arguments.add(new BasicNameValuePair("acc", type.toString()));

        try {
            post.setEntity(new UrlEncodedFormEntity(arguments));
            HttpResponse response = httpClient.execute(post);

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                result = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOGGER.info("Put into cache. key: " + cacheKey + " result: " + result.toString());
        this.cache.put(cacheKey, result);

        return result;
    }

    public boolean canWrite(String topic, String user, String client) {
        LOGGER.info("canWrite topic: " + topic + " user: " + user + " client: " + client);
        return this.canDo(topic, user, client, HttpAuthorizator.PUB);
    }

    public boolean canRead(String topic, String user, String client) {
        LOGGER.info("canRead topic: " + topic + " user: " + user + " client: " + client);
        return this.canDo(topic, user, client, HttpAuthorizator.SUB);
    }
}

