package io.polyglotted.common.es.discovery;

import com.amazonaws.AmazonClientException;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import io.polyglotted.common.config.SettingsHolder;
import io.polyglotted.common.es.discovery.Ec2Service.DISCOVERY_EC2;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static io.polyglotted.common.es.discovery.Ec2Service.DISCOVERY_EC2.HostType.*;
import static java.util.Collections.disjoint;

@Slf4j
public final class Ec2HostsProvider implements Closeable {
    private final AmazonEC2 client;
    private final boolean bindAnyGroup;
    private final Set<String> groups;
    private final Map<String, String> tags;
    private final Set<String> availabilityZones;
    private final String hostType;

    public static List<String> fetchEc2Addresses(Settings settings) throws IOException {
        try (Ec2HostsProvider provider = new Ec2HostsProvider(settings)) { return provider.fetchAddresses(); }
    }

    private Ec2HostsProvider(Settings settings) {
        this.client = Ec2ServiceImpl.client(settings);
        this.hostType = DISCOVERY_EC2.HOST_TYPE_SETTING.get(settings);
        this.bindAnyGroup = DISCOVERY_EC2.ANY_GROUP_SETTING.get(settings);
        this.groups = newHashSet(DISCOVERY_EC2.GROUPS_SETTING.get(settings));
        this.tags = DISCOVERY_EC2.TAG_SETTING.get(settings).getAsMap();
        this.availabilityZones = newHashSet(DISCOVERY_EC2.AVAILABILITY_ZONES_SETTING.get(settings));
        if (log.isDebugEnabled()) {
            log.debug("using host_type [{}], tags [{}], groups [{}] with any_group [{}], availability_zones [{}]",
                hostType, tags, groups, bindAnyGroup, availabilityZones);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) { client.shutdown(); }
        IdleConnectionReaper.shutdown();
    }

    private List<String> fetchAddresses() {
        List<String> addresses = new ArrayList<>();
        DescribeInstancesResult descInstances;
        try {
            descInstances = client.describeInstances(buildDescribeInstancesRequest());
        } catch (AmazonClientException e) {
            log.info("Exception while retrieving instance list from AWS API: {}", e.getMessage());
            log.debug("Full exception:", e);
            return addresses;
        }

        log.trace("building dynamic unicast discovery nodes...");
        for (Reservation reservation : descInstances.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                if (!groups.isEmpty()) {
                    List<GroupIdentifier> instanceSecurityGroups = instance.getSecurityGroups();
                    List<String> securityGroupNames = new ArrayList<>(instanceSecurityGroups.size());
                    List<String> securityGroupIds = new ArrayList<>(instanceSecurityGroups.size());
                    for (GroupIdentifier sg : instanceSecurityGroups) {
                        securityGroupNames.add(sg.getGroupName());
                        securityGroupIds.add(sg.getGroupId());
                    }
                    if (bindAnyGroup) {
                        if (disjoint(securityGroupNames, groups) && disjoint(securityGroupIds, groups)) {
                            log.trace("filtering out instance {} based on groups {}, not part of {}", instance.getInstanceId(),
                                instanceSecurityGroups, groups);
                            continue;
                        }
                    }
                    else {
                        if (!(securityGroupNames.containsAll(groups) || securityGroupIds.containsAll(groups))) {
                            log.trace("filtering out instance {} based on groups {}, does not include all of {}",
                                instance.getInstanceId(), instanceSecurityGroups, groups);
                            continue;
                        }
                    }
                }

                String address = null;
                if (hostType.equals(PRIVATE_DNS)) { address = instance.getPrivateDnsName(); }
                else if (hostType.equals(PRIVATE_IP)) { address = instance.getPrivateIpAddress(); }
                else if (hostType.equals(PUBLIC_DNS)) { address = instance.getPublicDnsName(); }
                else if (hostType.equals(PUBLIC_IP)) { address = instance.getPublicIpAddress(); }
                else if (hostType.startsWith(TAG_PREFIX)) {
                    String tagName = hostType.substring(TAG_PREFIX.length());
                    log.debug("reading hostname from [{}] instance tag", tagName);
                    List<Tag> tags = instance.getTags();
                    for (Tag tag : tags) {
                        if (tag.getKey().equals(tagName)) { address = tag.getValue(); log.debug("using [{}] as the instance address", address); }
                    }
                }
                else {
                    throw new IllegalArgumentException(hostType + " is unknown for discovery.ec2.host_type");
                }
                if (address != null) { addresses.add(address); }
            }
        }
        log.debug("using dynamic discovery addresses {}", addresses);
        return addresses;
    }

    private DescribeInstancesRequest buildDescribeInstancesRequest() {
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest()
            .withFilters(new Filter("instance-state-name").withValues("running", "pending"));
        for (Map.Entry<String, String> tagFilter : tags.entrySet()) {
            describeInstancesRequest.withFilters(new Filter("tag:" + tagFilter.getKey()).withValues(tagFilter.getValue()));
        }
        if (!availabilityZones.isEmpty()) {
            describeInstancesRequest.withFilters(new Filter("availability-zone").withValues(availabilityZones));
        }
        return describeInstancesRequest;
    }

    public static Settings buildSettings(SettingsHolder settingsHolder) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Object> e : settingsHolder.asProperties("es", false).entrySet()) {
            builder.put(e.getKey(), String.valueOf(e.getValue()));
        }
        return builder.build();
    }
}