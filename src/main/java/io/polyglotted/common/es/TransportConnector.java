package io.polyglotted.common.es;

import io.polyglotted.applauncher.settings.SettingsHolder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static io.polyglotted.common.es.discovery.Ec2HostsProvider.buildSettings;
import static io.polyglotted.common.es.discovery.Ec2HostsProvider.fetchEc2Addresses;
import static java.net.InetAddress.getByName;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.common.settings.Setting.Property.Shared;

@Slf4j
@SuppressWarnings("unused")
public abstract class TransportConnector {
    private static final Setting<Integer> TRANSPORT_PORT_SETTING = Setting.intSetting("transport.tcp.port", 9300, 0, 1 << 16, NodeScope, Shared);
    private static final Setting<List<String>> MASTER_NODES = Setting.listSetting("master.nodes", newArrayList("localhost"), String::toString, NodeScope);
    private static final Setting<String> EC2_DISCOVERY = new Setting<>("discovery.type", "", s -> s.toLowerCase(Locale.ROOT), NodeScope, Shared);
    private static final Setting<String> EC2_HOST_PROVIDER = new Setting<>("discovery.zen.hosts_provider", EC2_DISCOVERY, s -> s.toLowerCase(Locale.ROOT), NodeScope);

    @SneakyThrows
    public static TransportClient transportClient(SettingsHolder settingsHolder) {
        Settings settings = buildSettings(checkNotNull(settingsHolder));
        List<String> masterNodes = ((EC2_DISCOVERY.exists(settings) || EC2_HOST_PROVIDER.exists(settings))
            && "ec2".equals(EC2_HOST_PROVIDER.get(settings))) ? fetchEc2Addresses(settings) : MASTER_NODES.get(settings);
        return createFrom(settingsHolder, settings, masterNodes);
    }

    private static TransportClient createFrom(SettingsHolder holder, Settings settings, List<String> masterNodes) throws IOException {
        TransportClient client = new PreBuiltTransportClient(clientSettings(holder));
        int port = TransportConnector.TRANSPORT_PORT_SETTING.get(settings);
        for (String node : masterNodes) {
            InetAddress hostName = getByName(node);
            log.debug("adding transport master node {}:{}", hostName, port);
            client.addTransportAddress(new InetSocketTransportAddress(hostName, port));
        }
        return client;
    }

    private static Settings clientSettings(SettingsHolder settingsHolder) {
        return Settings.builder().put("cluster.name", settingsHolder.stringValue("es.cluster.name"))
            .put("client.transport.sniff", true).put("network.bind_host", 0).build();
    }
}