package io.polyglotted.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.*;
import static java.util.Objects.requireNonNull;

public abstract class MapperUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(READ_ENUMS_USING_TO_STRING, true).configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(FAIL_ON_NULL_FOR_PRIMITIVES, true).configure(ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        .configure(ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true).configure(ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false)
        .setSerializationInclusion(NON_NULL).setSerializationInclusion(NON_EMPTY)
        .setVisibility(new VisibilityChecker.Std(NONE, NONE, NONE, ANY, ANY));
    @SuppressWarnings("unchecked")
    private static final Class<Map<String, Object>> MAP_CLASS = (Class<Map<String, Object>>) new TypeToken<Map<String, Object>>() {}.getRawType();

    public static Map<String, Object> readToMap(String bytes) throws IOException { return MAPPER.readValue(bytes, MAP_CLASS); }

    public static String reqdStr(Map<String, Object> map, String key) { return requireNonNull((String) map.get(key), key + " is missing"); }
}