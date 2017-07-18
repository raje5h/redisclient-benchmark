package org.clients.benchmark;

import java.util.ArrayList;
import java.util.List;
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
import org.redisson.Redisson;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

@State(Scope.Benchmark)
public class ReddisonRedisClientBenchMark {

  private final static int BATCH_SIZE = 20;
  private final static byte[] KEY = "benchmark".getBytes();

  private String host = "127.0.0.1:6379";
  private RedissonClient client;

  @Setup
  public void setup() {
    Config config = new Config();
    config.useSingleServer().setTimeout(10000000)
        .setAddress(host).setConnectionPoolSize(10)
        .setConnectionMinimumIdleSize(10);
    config.setCodec(StringCodec.INSTANCE);
    client = Redisson.create(config);
//    client.getKeys().flushdb();
  }


  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public void asyncSetBatch() throws Exception {
    List<RFuture<Boolean>> rFutures = new ArrayList<>(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; i++) {
      rFutures.add(
          client.getSet("ABC").addAsync(i+"")
      );
    }
    for (RFuture future : rFutures) {
      future.get();
    }
  }


  @Benchmark
  public void syncSet() throws Exception {
    client.getSet("ABC").add(System.currentTimeMillis());
  }

  @TearDown
  public void tearDown() {
    client.shutdown();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ReddisonRedisClientBenchMark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
