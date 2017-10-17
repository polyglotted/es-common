package io.polyglotted.common.es;

import com.google.common.base.Splitter;
import io.polyglotted.applauncher.settings.SettingsHolder;
import io.polyglotted.common.es.rest.EsRestClient;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public class HighLevelConnector {

    @SneakyThrows
    public static ElasticClient highLevelClient(SettingsHolder settingsHolder) {
        RestClientBuilder builder = RestClient.builder(buildHosts(settingsHolder))
            .setMaxRetryTimeoutMillis(settingsHolder.intValue("es.max.retryTimeout.millis", 300_000));
        setCredentials(settingsHolder, builder);
        SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();
        RestClient restClient = builder.setFailureListener(sniffOnFailureListener).build();
        Sniffer sniffer = Sniffer.builder(restClient).setSniffAfterFailureDelayMillis(30000).build();
        sniffOnFailureListener.setSniffer(sniffer);
        return new EsRestClient(restClient, sniffer);
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    private static HttpHost[] buildHosts(SettingsHolder settingsHolder) {
        int port = settingsHolder.intValue("es.http.port", 9200);
        Iterable<String> masterNodes = Splitter.on(",").omitEmptyStrings().trimResults().
            split(settingsHolder.stringValue("es.master.nodes", "localhost"));
        return toArray(transform(masterNodes, node -> new HttpHost(requireNonNull(node), port, "http")), HttpHost.class);
    }

    private static void setCredentials(SettingsHolder settingsHolder, RestClientBuilder builder) {
        String userName = settingsHolder.stringValue("es.auth.username", null);
        String password = settingsHolder.stringValue("es.auth.password", null);
        if (nonNull(userName) && nonNull(password)) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            builder.setHttpClientConfigCallback(callback -> callback.setDefaultCredentialsProvider(credentialsProvider));
        }
    }
}