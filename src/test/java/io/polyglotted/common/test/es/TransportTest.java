package io.polyglotted.common.test.es;

import io.polyglotted.applauncher.settings.DefaultSettingsHolder;
import io.polyglotted.common.es.ElasticClient;
import org.testng.annotations.Test;

import static io.polyglotted.common.es.TransportConnector.transportClient;
import static org.testng.Assert.assertNotNull;

public class TransportTest {

    @Test
    public void createTransportWithoutEs() throws Exception {
        try (ElasticClient client = transportClient(new DefaultSettingsHolder("esdef.conf"))) { assertNotNull(client); }
    }
}