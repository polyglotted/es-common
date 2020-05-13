package io.polyglotted.common.es;

import com.google.common.base.Splitter;
import io.polyglotted.common.config.SettingsHolder;
import io.polyglotted.common.es.rest.EsRestClient;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

import javax.net.ssl.SSLContext;
import java.security.KeyStore;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.common.util.ResourceUtil.urlStream;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public class HighLevelConnector {

    public static ElasticClient highLevelClient(SettingsHolder settingsHolder) { return highLevelClient(settingsHolder, "es"); }

    @SneakyThrows
    public static ElasticClient highLevelClient(SettingsHolder settingsHolder, String prefix) {
        RestClientBuilder builder = RestClient.builder(buildHosts(settingsHolder, prefix))
            .setMaxRetryTimeoutMillis(settingsHolder.intValue(prefix + ".max.retryTimeout.millis", 300_000));
        if (settingsHolder.booleanValue(prefix + ".insecureSsl", false)) {
            builder.setHttpClientConfigCallback(clientBuilder -> clientBuilder
                    .setSSLHostnameVerifier(new NoopHostnameVerifier()).setSSLContext(predeterminedContext()));
        }

        setCredentials(settingsHolder, builder, prefix);
        return (settingsHolder.booleanValue(prefix + ".sniffer.enabled", false))
            ? sniffingClient(builder) : new EsRestClient(builder.build(), null);
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    private static HttpHost[] buildHosts(SettingsHolder settingsHolder, String prefix) {
        int port = settingsHolder.intValue(prefix + ".http.port", 9200);
        String scheme = settingsHolder.stringValue(prefix + ".scheme", "http");
        Iterable<String> masterNodes = Splitter.on(",").omitEmptyStrings().trimResults().
            split(settingsHolder.stringValue(prefix + ".master.nodes", "localhost"));
        return toArray(transform(masterNodes, node -> new HttpHost(requireNonNull(node), port, scheme)), HttpHost.class);
    }

    private static void setCredentials(SettingsHolder settingsHolder, RestClientBuilder builder, String prefix) {
        String userName = settingsHolder.stringValue(prefix + ".auth.username", null);
        String password = settingsHolder.stringValue(prefix + ".auth.password", null);
        if (nonNull(userName) && nonNull(password)) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            builder.setHttpClientConfigCallback(callback -> callback.setDefaultCredentialsProvider(credentialsProvider));
        }
    }

    private static EsRestClient sniffingClient(RestClientBuilder builder) {
        SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();
        RestClient restClient = builder.setFailureListener(sniffOnFailureListener).build();
        Sniffer sniffer = Sniffer.builder(restClient).setSniffAfterFailureDelayMillis(30000).build();
        sniffOnFailureListener.setSniffer(sniffer);
        return new EsRestClient(restClient, sniffer);
    }

    @SneakyThrows private static SSLContext predeterminedContext() {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(urlStream(HighLevelConnector.class, "abaci-ca.p12"), new char[0]);
        return SSLContexts.custom().loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();
    }
}