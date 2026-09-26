package com.navbon.ratelimiter.service;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.Refill;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;

@Service
public class RateLimiterService {

    private static final String RATE_LIMIT_BUCKET_KEY = "global_external_api_rate_limit";

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    @Value("${rate-limit.capacity:1000}")
    private long capacity;

    @Value("${rate-limit.refill-minutes:1}")
    private long refillMinutes;

    private RedisClient redisClient;
    private StatefulRedisConnection<String, byte[]> connection;
    private LettuceBasedProxyManager<String> proxyManager;

    @PostConstruct
    public void init() {
        String redisUrl = String.format("redis://%s:%d", redisHost, redisPort);
        this.redisClient = RedisClient.create(redisUrl);
        this.connection = redisClient.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));

        this.proxyManager = LettuceBasedProxyManager.builderFor(connection)
                .build();
    }

    public boolean tryConsume() {
        BucketConfiguration config = BucketConfiguration.builder()
                .addLimit(Bandwidth.classic(capacity, Refill.intervally(capacity, Duration.ofMinutes(refillMinutes))))
                .build();

        return proxyManager.builder()
                .build(RATE_LIMIT_BUCKET_KEY, config)
                .tryConsume(1);
    }

    @PreDestroy
    public void cleanup() {
        if (connection != null) connection.close();
        if (redisClient != null) redisClient.shutdown();
    }
}
