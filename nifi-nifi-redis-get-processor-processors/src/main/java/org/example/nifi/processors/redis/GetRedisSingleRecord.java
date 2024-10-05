package org.example.nifi.processors.redis;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessorInitializationContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.*;

@SupportsBatching
@Tags({"redis", "get", "single", "record"})
@CapabilityDescription("Reads a single record from Redis using a specified key.")
public class GetRedisSingleRecord extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_HOST = new PropertyDescriptor.Builder()
            .name("Redis Host")
            .description("The hostname of the Redis server.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIS_PORT = new PropertyDescriptor.Builder()
            .name("Redis Port")
            .description("The port of the Redis server.")
            .required(true)
            .defaultValue("6379")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIS_DB_INDEX = new PropertyDescriptor.Builder()
            .name("Redis DB Index")
            .description("The Redis database index to use.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAIN_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("Main Key Field Path")
            .description("The JSON path to the main key field, e.g., /id.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully retrieved from Redis.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to retrieve from Redis.")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(REDIS_HOST);
        descriptors.add(REDIS_PORT);
        descriptors.add(REDIS_DB_INDEX);
        descriptors.add(MAIN_KEY_FIELD);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Initialization logic if required
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String redisHost = context.getProperty(REDIS_HOST).getValue();
        final int redisPort = context.getProperty(REDIS_PORT).asInteger();
        final int dbIndex = context.getProperty(REDIS_DB_INDEX).asInteger();
        final String mainKeyFieldPath = context.getProperty(MAIN_KEY_FIELD).getValue();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try (Jedis jedis = new Jedis(redisHost, redisPort); InputStream in = session.read(flowFile)) {
            jedis.select(dbIndex);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(in);
            String mainKey = jsonNode.at(mainKeyFieldPath).asText(); // Extract key using the path

            if (mainKey != null && !mainKey.isEmpty()) {
                try {
                    String keyType = jedis.type(mainKey);
                    if (keyType.equals("string")) {
                        String value = jedis.get(mainKey);
                        if (value != null) {
                            FlowFile updatedFlowFile = session.putAttribute(flowFile, "redis.value", value);
                            session.transfer(updatedFlowFile, REL_SUCCESS);
                        } else {
                            session.transfer(flowFile, REL_FAILURE);
                        }
                    } else if (keyType.equals("hash")) {
                        Map<String, String> value = jedis.hgetAll(mainKey);
                        if (value != null && !value.isEmpty()) {
                            FlowFile updatedFlowFile = session.write(flowFile, out -> {
                                objectMapper.writeValue(out, value);
                            });
                            session.transfer(updatedFlowFile, REL_SUCCESS);
                        } else {
                            session.transfer(flowFile, REL_FAILURE);
                        }
                    } else {
                        getLogger().error("The key " + mainKey + " is not of type string or hash.");
                        session.transfer(flowFile, REL_FAILURE);
                    }
                } catch (JedisDataException e) {
                    getLogger().error("Failed to retrieve from Redis due to wrong type of value", e);
                    session.transfer(flowFile, REL_FAILURE);
                }
            } else {
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (Exception e) {
            getLogger().error("Failed to retrieve from Redis", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}