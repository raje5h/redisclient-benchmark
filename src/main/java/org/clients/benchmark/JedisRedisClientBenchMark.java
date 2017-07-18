package org.clients.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

@State(Scope.Benchmark)
public class JedisRedisClientBenchMark {

  private final static int BATCH_SIZE = 20;
  private final static byte[] KEY = "benchmark".getBytes();

  private String host = "127.0.0.1";
  private int port = 6379;
  private JedisPool pool;
  private ExecutorService executorService;

  @Setup
  public void setup() {
    pool = new JedisPool(new GenericObjectPoolConfig(), host, port);
    executorService = Executors.newFixedThreadPool(BATCH_SIZE);
    ;
  }

  @TearDown
  public void tearDown() {
    pool.close();
    executorService.shutdown();
  }

  @Benchmark
  public void asyncSetBatch() throws Exception {
    List<Callable<Void>> callables = new ArrayList<>(1);
    for (int i = 0; i < BATCH_SIZE; ++i) {
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Jedis jedis = pool.getResource();
          jedis.set(KEY, KEY);
          jedis.close();
          return null;
        }
      });
    }
    executorService.invokeAll(callables);
  }

  @Benchmark
  public void setSync() throws Exception {
    Jedis jedis = pool.getResource();
    jedis.set(("benchmark" + System.currentTimeMillis()).getBytes(), KEY);
    jedis.close();
  }



  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(JedisRedisClientBenchMark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
