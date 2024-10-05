# GetRedisSingleRecord Processor

## Overview
The **GetRedisSingleRecord** is a custom Apache NiFi processor designed to read a single record from a Redis database. This processor allows users to specify a key from a JSON flow file, which is then used to retrieve the associated data from Redis.

## Features
- Reads a record from Redis based on a key specified in the incoming JSON content.
- Configurable Redis host, port, and database index.
- Supports JSON key extraction to determine which Redis key to read.
- Ability to handle different Redis key types, including **hash** types.

## Configuration
The processor supports the following properties:

1. **Redis Host**: The hostname of the Redis server.
2. **Redis Port**: The port of the Redis server (default is `6379`).
3. **Redis DB Index**: The Redis database index to use (default is `0`).
4. **Main Key Field Path**: The JSON path to the main key field, e.g., `/id`, which specifies the key to use for retrieval.

## Relationships
The processor has the following relationships:

- **success**: FlowFiles that were successfully retrieved from Redis.
- **failure**: FlowFiles that failed to retrieve from Redis, either due to missing keys or incorrect data types.

## Usage Example
1. Add the **GetRedisSingleRecord** processor to your NiFi flow.
2. Configure the Redis connection properties, including **Redis Host**, **Redis Port**, and **Redis DB Index**.
3. Specify the **Main Key Field Path** to determine which field in the JSON should be used as the Redis key.
4. The processor will retrieve the value associated with the key from Redis and update the FlowFile content or attributes accordingly.

## Building and Deploying
1. Ensure that the **Jedis** and **Jackson** libraries are added as dependencies in your Maven project:

   ```xml
   <dependency>
       <groupId>redis.clients</groupId>
       <artifactId>jedis</artifactId>
       <version>4.3.1</version>
   </dependency>
   <dependency>
       <groupId>com.fasterxml.jackson.core</groupId>
       <artifactId>jackson-databind</artifactId>
       <version>2.14.0</version>
   </dependency>
   ```

2. Build the project using Maven:

   ```sh
   mvn clean install
   ```

3. Deploy the generated NAR file to the `lib` directory of your Apache NiFi instance.

## Notes
- This processor expects that the key specified in the **Main Key Field Path** is present in the incoming JSON content.
- The processor supports handling Redis **hash** types and writes the retrieved hash back into the FlowFile as JSON.