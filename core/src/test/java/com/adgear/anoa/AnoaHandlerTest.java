package com.adgear.anoa;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitx.framework.ListAssert;

public class AnoaHandlerTest {

  enum Meta { RUNTIME, OTHER, FAIL }

  final AnoaHandler<Meta> handler = AnoaHandler.withFn(
      t -> (t instanceof RuntimeException) ? Meta.RUNTIME : Meta.OTHER);

  @Test
  public void test() {
    AnoaHandler<Meta> handler = AnoaHandler.withFn(
        t -> (t instanceof RuntimeException) ? Meta.RUNTIME : Meta.OTHER);

    Assert.assertNotNull(handler.empty());
    Assert.assertEquals("derp", handler.of("derp").get());

    ListAssert.assertEquals(Collections.singletonList(Meta.RUNTIME),
                            handler.handle(new AnoaJacksonTypeException("hello"))
                                .collect(Collectors.toList()));
  }

  @Test
  public void testSuppliers() {
    Assert.assertEquals(Anoa.of("foo"),
                        handler.supplier(() -> "foo").get());
    Assert.assertEquals(Anoa.empty(Stream.of(Meta.OTHER)),
                        handler.supplierChecked(() -> {
                          throw new IOException();
                        }).get());
  }

  @Test
  public void testConsumers() {
    final AtomicLong lc = new AtomicLong(0L);
    Assert.assertEquals(Anoa.of(1L), handler.consumer(lc::addAndGet).apply(handler.of(1L)));
    Assert.assertEquals(1L, lc.get());
    Assert.assertEquals(Anoa.empty(Stream.of(Meta.OTHER)),
                        handler.consumerChecked(__ -> {
                          throw new IOException();
                        }).apply(handler.of(1L)));

    final AtomicLong lc2 = new AtomicLong(0L);
    Assert.assertEquals(Anoa.of(1L), handler.biConsumer((Long x, Long y) -> lc2.addAndGet(x + y))
        .apply(Anoa.of(1L), 1L));
    Assert.assertEquals(2L, lc2.get());

    Assert.assertEquals(Anoa.empty(Stream.of(Meta.OTHER)),
                        handler.biConsumerChecked((_1, _2) -> {
                          throw new IOException();
                        }).apply(Anoa.of(1L), 1L));
  }

  @Test
  public void testFunctions() {
    final AtomicLong lf = new AtomicLong(10L);
    Assert.assertEquals(Anoa.of(10L), handler.function(lf::getAndAdd).apply(handler.of(1L)));
    Assert.assertEquals(11L, lf.get());
    Assert.assertEquals(Anoa.empty(Stream.of(Meta.OTHER)),
                        handler.functionChecked(__ -> {
                          throw new IOException();
                        }).apply(handler.of(1L)));

    final AtomicLong lf2 = new AtomicLong(10L);
    Assert.assertEquals(Anoa.of(10L), handler.biFunction((Long x, Long y) -> lf2.getAndAdd(x + y))
        .apply(Anoa.of(1L), 1L));
    Assert.assertEquals(12L, lf2.get());

    Assert.assertEquals(Anoa.empty(Stream.of(Meta.OTHER)),
                        handler.biFunctionChecked((_1, _2) -> {
                          throw new IOException();
                        }).apply(Anoa.of(1L), 1L));
  }

  @Test
  public void testPredicates() {
    Assert.assertEquals(Anoa.empty(Stream.of(Meta.FAIL)),
                        handler.predicate(__ -> false, __ -> Stream.of(Meta.FAIL))
                            .apply(Anoa.of(1L)));
    Assert.assertEquals(Anoa.empty(Stream.of(Meta.OTHER)),
                        handler.predicateChecked(__ -> {
                          throw new IOException();
                        }, __ -> Stream.of(Meta.FAIL)).apply(Anoa.of(1L)));
  }
}
