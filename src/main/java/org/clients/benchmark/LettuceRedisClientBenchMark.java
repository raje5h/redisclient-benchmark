package org.clients.benchmark;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.codec.ByteArrayCodec;
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
import rx.Observable;

@State(Scope.Benchmark)
public class LettuceRedisClientBenchMark {

  private final static int BATCH_SIZE = 20;
  private final static byte[] KEY = "benchmark".getBytes();

  private RedisClient redisClient;
  private StatefulRedisConnection<byte[], byte[]> connection;
  private RedisFuture commands[];
  private Observable observables[];
  private String host = "127.0.0.1";
  private int port = 6379;

  @Setup
  public void setup() {
    redisClient = RedisClient.create(RedisURI.create(host, port));
    connection = redisClient.connect(ByteArrayCodec.INSTANCE);
    commands = new RedisFuture[BATCH_SIZE];
    observables = new Observable[BATCH_SIZE];
  }

  @TearDown
  public void tearDown() {

    connection.close();
    redisClient.shutdown();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public void test() throws Exception {

    for (int i = 0; i < BATCH_SIZE; i++) {
      commands[i] = connection.async().set(KEY, KEY);
    }


    for (int i = 0; i < BATCH_SIZE; i++) {
      commands[i].get();
    }
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public void asyncSetBatchFlush() throws Exception {

    connection.setAutoFlushCommands(false);

    for (int i = 0; i < BATCH_SIZE; i++) {
      commands[i] = connection.async().set(KEY, KEY);
    }

    connection.flushCommands();
    connection.setAutoFlushCommands(true);

    for (int i = 0; i < BATCH_SIZE; i++) {
      commands[i].get();
    }
  }

  @Benchmark
  public void syncSet() {
    connection.sync().set(KEY, KEY);
  }

  @Benchmark
  public void reactiveSet() {
    connection.reactive().set(KEY, KEY).toBlocking().single();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public void reactiveSetBatch() throws Exception {

    for (int i = 0; i < BATCH_SIZE; i++) {
      observables[i] = connection.reactive().set(KEY, KEY);
    }

    Observable.merge(observables).toBlocking().last();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public void reactiveSetBatchFlush() throws Exception {

    connection.setAutoFlushCommands(false);

    for (int i = 0; i < BATCH_SIZE; i++) {
      observables[i] = connection.reactive().set(KEY, KEY);
    }

    Observable.merge(observables).doOnSubscribe(() -> {

      connection.flushCommands();
      connection.setAutoFlushCommands(true);

    }).toBlocking().last();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(LettuceRedisClientBenchMark.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
