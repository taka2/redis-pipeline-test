package com.example.redis_pipeline_test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Component
public class MainRunner implements ApplicationRunner {

    @Autowired
    private RedisTemplate redisTemplate;

    @Value("${recordCount}")
    private int recordCount;

    private static final int TRY_COUNT = 3;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println("recordCount = " + recordCount);
        normalProcess(recordCount);
        pipelineProcess(recordCount);
        parallelStreamProcess(recordCount);

        System.exit(0);
    }

    /**
     * normal process (don't use pipeline)
     * @param recordCount
     */
    private void normalProcess(int recordCount) {
        // INIT
        redisTemplate.getConnectionFactory().getConnection().flushDb();

        // SET
        long totalTimeForSet = 0;
        for(int r=0; r<TRY_COUNT; r++) {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < recordCount; i++) {
                redisTemplate.opsForValue().set("key" + i, "value" + i);
            }
            long endTime = System.currentTimeMillis();
            totalTimeForSet += (endTime - startTime);
        }

        // GET
        long totalTimeForGet = 0;
        for(int r=0; r<TRY_COUNT; r++) {
            long startTime = System.currentTimeMillis();
            redisTemplate.keys("*").forEach(k -> {
                redisTemplate.opsForValue().get(k);
            });
            long endTime = System.currentTimeMillis();
            totalTimeForGet += (endTime - startTime);
        }

        System.out.println("[normal process]Average time for set = " + totalTimeForSet / TRY_COUNT);
        System.out.println("[normal process]Average time for get = " + totalTimeForGet / TRY_COUNT);
    }

    /**
     * pipeline process
     * @param recordCount
     */
    private void pipelineProcess(int recordCount) {
        // INIT
        redisTemplate.getConnectionFactory().getConnection().flushDb();

        // SET
        long totalTimeForSet = 0;
        for(int r=0; r<TRY_COUNT; r++) {
            long startTime = System.currentTimeMillis();

            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    for (int i = 0; i < recordCount; i++) {
                        connection.set(("key" + i).getBytes(), ("value" + i).getBytes());
                    }

                    return null;
                }
            });

            long endTime = System.currentTimeMillis();
            totalTimeForSet += (endTime - startTime);
        }

        // GET
        long totalTimeForGet = 0;
        for(int r=0; r<TRY_COUNT; r++) {
            long startTime = System.currentTimeMillis();
            Set<String> keys = redisTemplate.keys("*");

            redisTemplate.executePipelined(new RedisCallback<Object>() {
                @Override
                public Object doInRedis(RedisConnection connection) throws DataAccessException {
                    keys.forEach(k -> {
                        connection.get(k.getBytes());
                    });

                    return null;
                }
            });

            long endTime = System.currentTimeMillis();
            totalTimeForGet += (endTime - startTime);
        }

        System.out.println("[pipeline process]Average time for set = " + totalTimeForSet / TRY_COUNT);
        System.out.println("[pipeline process]Average time for get = " + totalTimeForGet / TRY_COUNT);
    }

    /**
     * parallel stream process
     * @param recordCount
     */
    private void parallelStreamProcess(int recordCount) {
        // INIT
        redisTemplate.getConnectionFactory().getConnection().flushDb();

        // SET
        long totalTimeForSet = 0;
        for(int r=0; r<TRY_COUNT; r++) {
            long startTime = System.currentTimeMillis();

            Stream.iterate(0, i->i+1).limit(recordCount).parallel().forEach(i -> {
                redisTemplate.opsForValue().set("key" + i, "value" + i);
            });

            long endTime = System.currentTimeMillis();
            totalTimeForSet += (endTime - startTime);
        }

        // GET
        long totalTimeForGet = 0;
        for(int r=0; r<TRY_COUNT; r++) {
            long startTime = System.currentTimeMillis();
            Set<String> keys = redisTemplate.keys("*");

            Stream.iterate(0, i->i).limit(recordCount).parallel().forEach(i -> {
                redisTemplate.opsForValue().get("key" + i);
            });

            long endTime = System.currentTimeMillis();
            totalTimeForGet += (endTime - startTime);
        }

        System.out.println("[parallel stream process]Average time for set = " + totalTimeForSet / TRY_COUNT);
        System.out.println("[parallel stream process]Average time for get = " + totalTimeForGet / TRY_COUNT);
    }
}
