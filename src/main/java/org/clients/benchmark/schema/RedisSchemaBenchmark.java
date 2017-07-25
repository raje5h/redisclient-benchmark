package org.clients.benchmark.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by rajesh.chowdary on 25/07/17.
 */
@State(Scope.Benchmark)
public class RedisSchemaBenchmark {
    private final String host = "127.0.0.1";
    private final int port = 6379;
    private final int keyspaceSize = 1000000;
    private final String listingId = "LSTRANDOM%s_meta";
    private final String lznKey = "LSTRANDOM%s_lzn";
    private Map<String, Integer> fieldVals;
    private final Random random = new Random();
    private final int PARALLELISM = 1;
    private ExecutorService executorService;
    private Jedis jedis;

    private static boolean bootstrapped = false;

    @Setup
    public void setup() {
        executorService = Executors.newFixedThreadPool(PARALLELISM);

        fieldVals = Maps.newHashMap();
        fieldVals.put("av", 1);
        fieldVals.put("lv", 1);
        fieldVals.put("st", 9);
        fieldVals.put("ss", 1);
        fieldVals.put("sl", 6);
        fieldVals.put("af", 1);
        fieldVals.put("fa", 1);
        fieldVals.put("qs", 25);
        fieldVals.put("x1", 25);

        jedis = new Jedis(host, port);

        if (!bootstrapped) {
            List<Future<?>> submits = Lists.newLinkedList();
            for (int p = 0; p < PARALLELISM; p++) {
                int finalP = p;
                submits.add(executorService.submit(() -> {
                    Jedis localJedis = new Jedis(host, port);
                    for (int i = finalP * PARALLELISM; i < finalP * PARALLELISM + (keyspaceSize / PARALLELISM); i++) {
                        Pipeline pipeline = localJedis.pipelined();
                        String num = String.format("%016d", i);
                        String key = String.format(listingId, num);
                        for (String field : fieldVals.keySet())
                            pipeline.hset(key, field, Integer.toString(random.nextInt(fieldVals.get(field))));
//                        for (int zone = 0; zone < 256; zone++)
//                            pipeline.hset(key, Integer.toString(zone), Integer.toString(random.nextInt(3)));
                        pipeline.setbit(String.format(lznKey, num), 0, true);
                        pipeline.setbit(String.format(lznKey, num), 512, false);
                        pipeline.sync();
                    }
                }));
            }

            for (Future<?> submit : submits) {
                try {
                    submit.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(jedis.info("memory"));
            bootstrapped = true;
        }

    }

    @TearDown
    public void tearDown() throws Exception {
        //jedis.flushAll();
        jedis.close();
        executorService.shutdown();
    }

    @Benchmark
    public void hget() throws Exception {
        String key = String.format(listingId, random.nextInt(keyspaceSize));
        String[] fieldArray = new String[fieldVals.size()];
        fieldArray = fieldVals.keySet().toArray(fieldArray);
        String field = fieldArray[random.nextInt(fieldArray.length)];
        jedis.hget(key, field);
    }

//    @Benchmark
//    public void getLZN() throws Exception {
//        int num = random.nextInt(keyspaceSize);
//        String key2 = String.format(lznKey, num);
//        Pipeline pipelined = jedis.pipelined();
//        pipelined.multi();
//        pipelined.getbit(key2, 0);
//        pipelined.getbit(key2, 1);
//        pipelined.exec();
//        pipelined.syncAndReturnAll();
//    }

    @Benchmark
    public void hgetall() throws Exception {
        int num = random.nextInt(keyspaceSize);
        String key1 = String.format(listingId, num);
        String key2 = String.format(lznKey, num);
        Pipeline pipelined = jedis.pipelined();
        pipelined.hgetAll(key1);
//        pipelined.get(key2);
        pipelined.sync();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RedisSchemaBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
