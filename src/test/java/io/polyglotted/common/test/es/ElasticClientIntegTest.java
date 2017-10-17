package io.polyglotted.common.test.es;

import io.polyglotted.applauncher.settings.DefaultSettingsHolder;
import io.polyglotted.common.es.ElasticClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static io.polyglotted.common.es.HighLevelConnector.highLevelClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ElasticClientIntegTest {
    private ElasticClient elasticClient;

    @BeforeMethod
    public void setUp() throws Exception { elasticClient = highLevelClient(new DefaultSettingsHolder()); }

    @AfterMethod
    public void tearDown() throws Exception { elasticClient.close(); }

    @Test
    public void testClusterHealth() throws Exception {
        Map<String, Object> health = elasticClient.clusterHealth();
        assertThat(health.keySet(), containsInAnyOrder("cluster_name", "status", "timed_out", "number_of_nodes", "number_of_data_nodes",
            "active_primary_shards", "active_shards", "relocating_shards", "initializing_shards", "unassigned_shards", "delayed_unassigned_shards",
            "number_of_pending_tasks", "number_of_in_flight_fetch", "task_max_waiting_in_queue_millis", "active_shards_percent_as_number"));
    }
}