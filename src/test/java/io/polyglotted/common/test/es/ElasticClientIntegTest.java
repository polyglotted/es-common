package io.polyglotted.common.test.es;

import io.polyglotted.common.config.SettingsHolder;
import io.polyglotted.common.es.ElasticClient;
import org.testng.annotations.Test;

import java.util.Map;

import static io.polyglotted.common.es.HighLevelConnector.highLevelClient;
import static io.polyglotted.common.es.TransportConnector.transportClient;
import static io.polyglotted.common.util.ResourceUtil.readResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class ElasticClientIntegTest {

    @Test
    public void testEs5Highlevel() throws Exception {
        try (ElasticClient elasticClient = highLevelClient(new SettingsHolder("esdef"))) {
            checkHealth(elasticClient);
            checkPipelineLifecycle(elasticClient);
            checkTemplateLifecycle(elasticClient);
        }
    }

    @Test
    public void testEs5Transport() throws Exception {
        try (ElasticClient elasticClient = transportClient(new SettingsHolder("esdef"))) {
            checkHealth(elasticClient);
            checkPipelineLifecycle(elasticClient);
            checkTemplateLifecycle(elasticClient);
        }
    }

    private static void checkHealth(ElasticClient elasticClient) {
        Map<String, Object> health = elasticClient.clusterHealth();
        assertThat(health.keySet(), containsInAnyOrder("cluster_name", "status", "timed_out", "number_of_nodes", "number_of_data_nodes",
                "active_primary_shards", "active_shards", "relocating_shards", "initializing_shards", "unassigned_shards", "delayed_unassigned_shards",
                "number_of_pending_tasks", "number_of_in_flight_fetch", "task_max_waiting_in_queue_millis", "active_shards_percent_as_number"));
    }

    private void checkPipelineLifecycle(ElasticClient client) {
        String pipeline = "mypipe";
        assertThat(client.pipelineExists(pipeline), is(false));
        client.buildPipeline(pipeline, read("pipeline-source.json"));
        assertThat(client.pipelineExists(pipeline), is(true));
        client.deletePipeline(pipeline);
        assertThat(client.pipelineExists(pipeline), is(false));
    }

    private void checkTemplateLifecycle(ElasticClient client) {
        String template = "1234absdffew234";
        assertThat(client.templateExists(template), is(false));
        client.putTemplate(template, read("template-source-5.json"));
        assertThat(client.templateExists(template), is(true));
        client.putTemplate(template, read("template-update-5.json"));
        client.deleteTemplate(template);
        assertThat(client.templateExists(template), is(false));
    }

    private static String read(String resource) {
        return readResource(ElasticClientIntegTest.class, resource);
    }
}