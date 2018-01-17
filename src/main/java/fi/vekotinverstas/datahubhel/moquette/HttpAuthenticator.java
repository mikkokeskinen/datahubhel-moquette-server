package fi.vekotinverstas.datahubhel.moquette;

import io.moquette.spi.security.IAuthenticator;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class HttpAuthenticator implements IAuthenticator {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAuthenticator.class);

    private String authUri = null;

    public HttpAuthenticator() {
        LOGGER.info("Getting MoquetteHttpAuthenticatorUri from context");
        try {
            this.authUri = (String) new InitialContext().lookup("java:comp/env/MoquetteHttpAuthenticatorUri");
            LOGGER.info("Auth URI: " + this.authUri);
        } catch (NamingException e) {
            LOGGER.warn("Couldn't find MoquetteHttpAuthenticatorUri setting from context");
        }
    }

    public boolean checkValid(String clientId, String username, byte[] password) {
        LOGGER.info(String.format("Trying username '%s'", username));
        if (this.authUri == null) {
            LOGGER.warn("MoquetteHttpAuthorizatorUri not set. Denying access.");
            return false;
        }

        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(this.authUri);

        List<NameValuePair> arguments = new ArrayList<>(2);
        arguments.add(new BasicNameValuePair("username", username));
        arguments.add(new BasicNameValuePair("password",  new String(password, Charset.forName("UTF-8"))));

        try {
            post.setEntity(new UrlEncodedFormEntity(arguments));
            HttpResponse response = client.execute(post);

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }
}
