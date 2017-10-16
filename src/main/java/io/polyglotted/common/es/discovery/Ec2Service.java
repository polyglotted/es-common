package io.polyglotted.common.es.discovery;

import com.amazonaws.Protocol;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static com.amazonaws.ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;
import static org.elasticsearch.common.settings.Setting.Property.*;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

@SuppressWarnings({"deprecation"})
interface Ec2Service {
    Setting<SecureString> KEY_SETTING = new Setting<>("cloud.aws.access_key", "", SecureString::new, NodeScope, Filtered, Shared);
    Setting<SecureString> SECRET_SETTING = new Setting<>("cloud.aws.secret_key", "", SecureString::new, NodeScope, Filtered, Shared);
    Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), NodeScope, Shared);
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("cloud.aws.proxy.host", NodeScope, Shared);
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("cloud.aws.proxy.port", 80, 0, 1 << 16, NodeScope, Shared);
    Setting<SecureString> PROXY_USERNAME_SETTING = new Setting<>("cloud.aws.proxy.username", "", SecureString::new, NodeScope, Filtered, Shared);
    Setting<SecureString> PROXY_PASSWORD_SETTING = new Setting<>("cloud.aws.proxy.password", "", SecureString::new, NodeScope, Filtered, Shared);
    Setting<String> SIGNER_SETTING = Setting.simpleString("cloud.aws.signer", NodeScope, Shared);
    Setting<String> REGION_SETTING = new Setting<>("cloud.aws.region", "", s -> s.toLowerCase(Locale.ROOT), NodeScope, Shared);
    Setting<TimeValue> READ_TIMEOUT = Setting.timeSetting("cloud.aws.read_timeout", timeValueMillis(DEFAULT_SOCKET_TIMEOUT), NodeScope, Shared);

    interface CLOUD_EC2 {
        Setting<SecureString> KEY_SETTING = new Setting<>("cloud.aws.ec2.access_key", Ec2Service.KEY_SETTING, SecureString::new, NodeScope, Filtered);
        Setting<SecureString> SECRET_SETTING = new Setting<>("cloud.aws.ec2.secret_key", Ec2Service.SECRET_SETTING, SecureString::new, NodeScope, Filtered);
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("cloud.aws.ec2.protocol", Ec2Service.PROTOCOL_SETTING, s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), NodeScope);
        Setting<String> PROXY_HOST_SETTING = new Setting<>("cloud.aws.ec2.proxy.host", Ec2Service.PROXY_HOST_SETTING, Function.identity(), NodeScope);
        Setting<Integer> PROXY_PORT_SETTING = new Setting<>("cloud.aws.ec2.proxy.port", Ec2Service.PROXY_PORT_SETTING, s -> Setting.parseInt(s, 0, 1 << 16, "cloud.aws.ec2.proxy.port"), NodeScope);
        Setting<SecureString> PROXY_USERNAME_SETTING = new Setting<>("cloud.aws.ec2.proxy.username", Ec2Service.PROXY_USERNAME_SETTING, SecureString::new, NodeScope, Filtered);
        Setting<SecureString> PROXY_PASSWORD_SETTING = new Setting<>("cloud.aws.ec2.proxy.password", Ec2Service.PROXY_PASSWORD_SETTING, SecureString::new, NodeScope, Filtered);
        Setting<String> SIGNER_SETTING = new Setting<>("cloud.aws.ec2.signer", Ec2Service.SIGNER_SETTING, Function.identity(), NodeScope);
        Setting<String> REGION_SETTING = new Setting<>("cloud.aws.ec2.region", Ec2Service.REGION_SETTING, s -> s.toLowerCase(Locale.ROOT), NodeScope);
        Setting<TimeValue> READ_TIMEOUT = Setting.timeSetting("cloud.aws.ec2.read_timeout", Ec2Service.READ_TIMEOUT, NodeScope);
    }

    interface DISCOVERY_EC2 {
        class HostType {
            static final String PRIVATE_IP = "private_ip";
            static final String PUBLIC_IP = "public_ip";
            static final String PRIVATE_DNS = "private_dns";
            static final String PUBLIC_DNS = "public_dns";
            static final String TAG_PREFIX = "tag:";
        }

        Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.secureString("discovery.ec2.access_key", CLOUD_EC2.KEY_SETTING);
        Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.secureString("discovery.ec2.secret_key", CLOUD_EC2.SECRET_SETTING);
        Setting<Protocol> PROTOCOL_SETTING = new Setting<>("discovery.ec2.protocol", CLOUD_EC2.PROTOCOL_SETTING, s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), NodeScope);
        Setting<String> PROXY_HOST_SETTING = new Setting<>("discovery.ec2.proxy.host", CLOUD_EC2.PROXY_HOST_SETTING, Function.identity(), NodeScope);
        Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("discovery.ec2.proxy.port", CLOUD_EC2.PROXY_PORT_SETTING, 0, NodeScope);
        Setting<SecureString> PROXY_USERNAME_SETTING = SecureSetting.secureString("discovery.ec2.proxy.username", CLOUD_EC2.PROXY_USERNAME_SETTING);
        Setting<SecureString> PROXY_PASSWORD_SETTING = SecureSetting.secureString("discovery.ec2.proxy.password", CLOUD_EC2.PROXY_PASSWORD_SETTING);
        Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting("discovery.ec2.read_timeout", CLOUD_EC2.READ_TIMEOUT, NodeScope);
        Setting<String> HOST_TYPE_SETTING = new Setting<>("discovery.ec2.host_type", HostType.PRIVATE_IP, Function.identity(), NodeScope);
        Setting<Boolean> ANY_GROUP_SETTING = Setting.boolSetting("discovery.ec2.any_group", true, NodeScope);
        Setting<List<String>> GROUPS_SETTING = Setting.listSetting("discovery.ec2.groups", new ArrayList<>(), String::toString, NodeScope);
        Setting<List<String>> AVAILABILITY_ZONES_SETTING = Setting.listSetting("discovery.ec2.availability_zones", Collections.emptyList(), String::toString, NodeScope);
        Setting<Settings> TAG_SETTING = Setting.groupSetting("discovery.ec2.tag.", NodeScope);
    }
}