package com.adgear.anoa;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitx.framework.ListAssert;

public class AnoaTest {


  static private final Anoa<Integer, String> ANOA = Anoa.of(1, Stream.of("foo", "bar"));
  static private final Anoa<Integer, String> EMPTY = Anoa.empty(Stream.of("empty!"));

  @Test
  public void testOf() {
    Assert.assertNotNull(ANOA);
    Assert.assertNotNull(EMPTY);
  }

  @Test
  public void testGet() {
    Assert.assertEquals(1, (Object) ANOA.get());
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetEmpty() throws Exception {
    EMPTY.get();
  }

  @Test
  public void testAsOptional() {
    Assert.assertEquals(Optional.of(1), ANOA.asOptional());
    Assert.assertEquals(Optional.empty(), EMPTY.asOptional());
  }

  @Test
  public void testAsStream() {
    ListAssert.assertEquals(Collections.singletonList(1),
                            ANOA.asStream().collect(Collectors.toList()));
    ListAssert.assertEquals(Collections.emptyList(),
                            EMPTY.asStream().collect(Collectors.toList()));
  }

  @Test
  public void testMeta() {
    ListAssert.assertEquals(Arrays.asList("foo", "bar"),
                            ANOA.meta().collect(Collectors.toList()));
  }

  @Test
  public void testIsPresent() {
    Assert.assertTrue(ANOA.isPresent());
    Assert.assertFalse(EMPTY.isPresent());
  }

  @Test
  public void testIfPresent() {
    AtomicLong atomicLong = new AtomicLong(0L);
    ANOA.ifPresent(atomicLong::getAndAdd);
    Assert.assertEquals(1L, atomicLong.get());

    atomicLong = new AtomicLong(0L);
    EMPTY.ifPresent(atomicLong::getAndAdd);
    Assert.assertEquals(0L, atomicLong.get());
  }

  @Test
  public void testOrElse() {
    Assert.assertEquals(1, (int) ANOA.orElse(2));
    Assert.assertEquals(2, (int) EMPTY.orElse(2));
  }

  @Test
  public void testOrElseGet() {
    Assert.assertEquals(1, (int) ANOA.orElseGet(() -> 2));
    Assert.assertEquals(2, (int) EMPTY.orElseGet(() -> 2));
  }

  @Test(expected = IllegalStateException.class)
  public void testOrElseThrow() {
    Assert.assertEquals(1, (Object) ANOA.orElseThrow(RuntimeException::new));
    EMPTY.orElseThrow(IllegalStateException::new);
  }

  @Test
  public void testFilter() {
    Assert.assertFalse(ANOA.filter(i -> (i % 2 == 0)).isPresent());
    Assert.assertTrue(ANOA.filter(i -> (i % 2 != 0)).isPresent());
    Assert.assertFalse(EMPTY.filter(i -> (i % 2 == 0)).isPresent());
    Assert.assertFalse(EMPTY.filter(i -> (i % 2 != 0)).isPresent());
  }

  @Test
  public void testMap() {
    Assert.assertEquals(2, (Object) ANOA.map(i -> i + 1).get());
    Assert.assertFalse(EMPTY.map(i -> i + 1).isPresent());
  }

  @Test
  public void testFlatMap() {
    Anoa<Integer, String> result = ANOA.flatMap(i -> Anoa.of(i + 1, Stream.of("baz")));
    Assert.assertEquals(2, (int) result.get());
    ListAssert.assertEquals(Arrays.asList("foo", "bar", "baz"),
                            result.meta().collect(Collectors.toList()));

    ListAssert.assertEquals(EMPTY.meta().collect(Collectors.toList()),
                            EMPTY.flatMap(i -> Anoa.of(i + 1, Stream.of("baz"))).meta()
                                .collect(Collectors.toList()));
  }
}
