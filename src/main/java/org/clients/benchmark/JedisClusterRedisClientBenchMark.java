package org.clients.benchmark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

@State(Scope.Benchmark)
public class JedisClusterRedisClientBenchMark {

  private final static int BATCH_SIZE = 20;
  private final static byte[] KEY = "benchmark".getBytes();

  private String host = "127.0.0.1";
  private int port = 30001;
  private JedisCluster jedisCluster;
  private ExecutorService executorService;

  @Setup
  public void setup() {
    Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
    jedisClusterNodes.add(new HostAndPort(host, port));
    jedisCluster = new JedisCluster(jedisClusterNodes);
    executorService = Executors.newFixedThreadPool(BATCH_SIZE);
    ;
  }

  @TearDown
  public void tearDown() throws Exception {
    jedisCluster.close();
    executorService.shutdown();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public void asyncSetBatch() throws Exception {
    List<Callable<Void>> callables = new ArrayList<>(1);
    for (int i = 0; i < BATCH_SIZE; ++i) {
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          jedisCluster.set(KEY, KEY);
          return null;
        }
      });
    }
    executorService.invokeAll(callables);
  }

  @Benchmark
  public void setSync() throws Exception {
    jedisCluster.set(("benchmark" + System.currentTimeMillis()).getBytes(), KEY);
  }


  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(JedisClusterRedisClientBenchMark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
