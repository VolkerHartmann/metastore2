/*
 * Copyright 2026 Karlsruhe Institute of Technology.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.kit.datamanager.metastore2.util;

import org.junit.Assert;
import org.junit.Test;

public class SemanticVersionTest {

  @Test
  public void parseValidString() {
    try {
      SemanticVersion.parse("0.0.0");
      SemanticVersion.parse("1.2.3");
      SemanticVersion.parse(" 1.2.3 ");
      SemanticVersion.parse("10.11.12");
      Assert.assertTrue(true);
    } catch (IllegalArgumentException e) {
      // Unexpected exception
      Assert.fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  public void parseInvalidString() {
    String[] invalidVersions = {
            null, "", " ", "1", "2.", "3.4", "4.5.", "1. 2.3", "1.2. 3", "01.2.3", "1.2.-3", "1.-2.3", "-1.2.3", "1.2.3-alpha", "1.2.3+build", "1.2.3-SNAPSHOT",
            "", " ", null, "1..3", "1.2.", ".2.3", "a.b.c", "1.2.2147483648"
    };
    for (String version : invalidVersions) {
      try {
        SemanticVersion.parse(version);
        Assert.fail("Expected IllegalArgumentException for input: " + version);
      } catch (IllegalArgumentException | NullPointerException e) {
        // Expected exception, test passes
        Assert.assertTrue(true);
      }
    }
  }

  @Test
  public void tryParse() {
    Assert.assertTrue(SemanticVersion.tryParse("1.2.3").isPresent());
    Assert.assertFalse(SemanticVersion.tryParse("invalid").isPresent());
    Assert.assertFalse(SemanticVersion.tryParse(null).isPresent());
  }

  @Test
  public void getVersionComponent() {
    SemanticVersion version = SemanticVersion.parse("1.2.3");
    Assert.assertEquals(1, version.getVersionComponent(SemanticVersion.INCREMENT_LEVEL.MAJOR));
    Assert.assertEquals(2, version.getVersionComponent(SemanticVersion.INCREMENT_LEVEL.MINOR));
    Assert.assertEquals(3, version.getVersionComponent(SemanticVersion.INCREMENT_LEVEL.PATCH));
    Assert.assertEquals(1, version.getMajor());
    Assert.assertEquals(2, version.getMinor());
    Assert.assertEquals(3, version.getPatch());
    try {
      version.getVersionComponent(null);
      Assert.fail("Expected NullPointerException for null increment level");
    } catch (NullPointerException e) {
      // Expected exception, test passes
      Assert.assertTrue(true);
    }
  }

  @Test
  public void compareTo() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    SemanticVersion v2 = SemanticVersion.parse("1.2.4");
    SemanticVersion v3 = SemanticVersion.parse("1.3.0");
    SemanticVersion v4 = SemanticVersion.parse("2.0.0");
    Assert.assertTrue(v1.compareTo(v1) == 0);
    Assert.assertTrue(v1.compareTo(v2) < 0);
    Assert.assertTrue(v1.compareTo(v3) < 0);
    Assert.assertTrue(v1.compareTo(v4) < 0);
    Assert.assertTrue(v2.compareTo(v1) > 0);
    Assert.assertTrue(v2.compareTo(v2) == 0);
    Assert.assertTrue(v2.compareTo(v3) < 0);
    Assert.assertTrue(v2.compareTo(v4) < 0);
    Assert.assertTrue(v3.compareTo(v1) > 0);
    Assert.assertTrue(v3.compareTo(v2) > 0);
    Assert.assertTrue(v3.compareTo(v3) == 0);
    Assert.assertTrue(v3.compareTo(v4) < 0);
    Assert.assertTrue(v4.compareTo(v1) > 0);
    Assert.assertTrue(v4.compareTo(v2) > 0);
    Assert.assertTrue(v4.compareTo(v3) > 0);
    Assert.assertTrue(v4.compareTo(v4) == 0);
  }

  @Test
  public void getDifferenceLevel() {
    SemanticVersion v1 = SemanticVersion.parse("1.0.1");
    SemanticVersion v2 = SemanticVersion.parse("1.0.2");
    SemanticVersion v3 = SemanticVersion.parse("1.1.0");
    SemanticVersion v4 = SemanticVersion.parse("2.0.0");
    Assert.assertNull(v1.getDifferenceLevel(v1));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.PATCH, v1.getDifferenceLevel(v2));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MINOR, v1.getDifferenceLevel(v3));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MAJOR, v1.getDifferenceLevel(v4));
    Assert.assertNull(v2.getDifferenceLevel(v2));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.PATCH, v2.getDifferenceLevel(v1));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MINOR, v2.getDifferenceLevel(v3));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MAJOR, v2.getDifferenceLevel(v4));
    Assert.assertNull(v3.getDifferenceLevel(v3));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MINOR, v3.getDifferenceLevel(v1));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MINOR, v3.getDifferenceLevel(v2));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MAJOR, v3.getDifferenceLevel(v4));
    Assert.assertNull(v4.getDifferenceLevel(v4));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MAJOR, v4.getDifferenceLevel(v1));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MAJOR, v4.getDifferenceLevel(v2));
    Assert.assertEquals(SemanticVersion.INCREMENT_LEVEL.MAJOR, v4.getDifferenceLevel(v3));
  }

  @Test
  public void isBefore() {
    SemanticVersion v1 = SemanticVersion.parse("1.0.1");
    SemanticVersion v2 = SemanticVersion.parse("1.0.2");
    SemanticVersion v3 = SemanticVersion.parse("1.1.0");
    SemanticVersion v4 = SemanticVersion.parse("2.0.0");
    Assert.assertFalse(v1.isBefore(v1));
    Assert.assertTrue(v1.isBefore(v2));
    Assert.assertTrue(v1.isBefore(v3));
    Assert.assertTrue(v1.isBefore(v4));
    Assert.assertFalse(v2.isBefore(v1));
    Assert.assertFalse(v2.isBefore(v2));
    Assert.assertTrue(v2.isBefore(v3));
    Assert.assertTrue(v2.isBefore(v4));
    Assert.assertFalse(v3.isBefore(v1));
    Assert.assertFalse(v3.isBefore(v2));
    Assert.assertFalse(v3.isBefore(v3));
    Assert.assertTrue(v3.isBefore(v4));
    Assert.assertFalse(v4.isBefore(v1));
    Assert.assertFalse(v4.isBefore(v2));
    Assert.assertFalse(v4.isBefore(v3));
    Assert.assertFalse(v4.isBefore(v4));
  }

  @Test
  public void isAfter() {
    SemanticVersion v1 = SemanticVersion.parse("1.0.1");
    SemanticVersion v2 = SemanticVersion.parse("1.0.2");
    SemanticVersion v3 = SemanticVersion.parse("1.1.0");
    SemanticVersion v4 = SemanticVersion.parse("2.0.0");
    Assert.assertFalse(v1.isAfter(v1));
    Assert.assertFalse(v1.isAfter(v2));
    Assert.assertFalse(v1.isAfter(v3));
    Assert.assertFalse(v1.isAfter(v4));
    Assert.assertTrue(v2.isAfter(v1));
    Assert.assertFalse(v2.isAfter(v2));
    Assert.assertFalse(v2.isAfter(v3));
    Assert.assertFalse(v2.isAfter(v4));
    Assert.assertTrue(v3.isAfter(v1));
    Assert.assertTrue(v3.isAfter(v2));
    Assert.assertFalse(v3.isAfter(v3));
    Assert.assertFalse(v3.isAfter(v4));
    Assert.assertTrue(v4.isAfter(v1));
    Assert.assertTrue(v4.isAfter(v2));
    Assert.assertTrue(v4.isAfter(v3));
    Assert.assertFalse(v4.isAfter(v4));
  }

  @Test
  public void isAtLeast() {
    SemanticVersion v1 = SemanticVersion.parse("1.0.1");
    SemanticVersion v2 = SemanticVersion.parse("1.0.2");
    SemanticVersion v3 = SemanticVersion.parse("1.1.0");
    SemanticVersion v4 = SemanticVersion.parse("2.0.0");
    Assert.assertTrue(v1.isAtLeast(v1));
    Assert.assertFalse(v1.isAtLeast(v2));
    Assert.assertFalse(v1.isAtLeast(v3));
    Assert.assertFalse(v1.isAtLeast(v4));
    Assert.assertTrue(v2.isAtLeast(v1));
    Assert.assertTrue(v2.isAtLeast(v2));
    Assert.assertFalse(v2.isAtLeast(v3));
    Assert.assertFalse(v2.isAtLeast(v4));
    Assert.assertTrue(v3.isAtLeast(v1));
    Assert.assertTrue(v3.isAtLeast(v2));
    Assert.assertTrue(v3.isAtLeast(v3));
    Assert.assertFalse(v3.isAtLeast(v4));
    Assert.assertTrue(v4.isAtLeast(v1));
    Assert.assertTrue(v4.isAtLeast(v2));
    Assert.assertTrue(v4.isAtLeast(v3));
    Assert.assertTrue(v4.isAtLeast(v4));
  }

  @Test
  public void isAtMost() {
    SemanticVersion v1 = SemanticVersion.parse("1.0.1");
    SemanticVersion v2 = SemanticVersion.parse("1.0.2");
    SemanticVersion v3 = SemanticVersion.parse("1.1.0");
    SemanticVersion v4 = SemanticVersion.parse("2.0.0");
    Assert.assertTrue(v1.isAtMost(v1));
    Assert.assertTrue(v1.isAtMost(v2));
    Assert.assertTrue(v1.isAtMost(v3));
    Assert.assertTrue(v1.isAtMost(v4));
    Assert.assertFalse(v2.isAtMost(v1));
    Assert.assertTrue(v2.isAtMost(v2));
    Assert.assertTrue(v2.isAtMost(v3));
    Assert.assertTrue(v2.isAtMost(v4));
    Assert.assertFalse(v3.isAtMost(v1));
    Assert.assertFalse(v3.isAtMost(v2));
    Assert.assertTrue(v3.isAtMost(v3));
    Assert.assertTrue(v3.isAtMost(v4));
    Assert.assertFalse(v4.isAtMost(v1));
    Assert.assertFalse(v4.isAtMost(v2));
    Assert.assertFalse(v4.isAtMost(v3));
    Assert.assertTrue(v4.isAtMost(v4));
  }

  @Test
  public void increment() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    Assert.assertEquals(SemanticVersion.parse("1.2.4"), v1.increment(SemanticVersion.INCREMENT_LEVEL.PATCH));
    Assert.assertEquals(SemanticVersion.parse("1.3.0"), v1.increment(SemanticVersion.INCREMENT_LEVEL.MINOR));
    Assert.assertEquals(SemanticVersion.parse("2.0.0"), v1.increment(SemanticVersion.INCREMENT_LEVEL.MAJOR));
    try {
      v1.increment(null);
      Assert.fail("Expected NullPointerException for null increment level");
    } catch (NullPointerException e) {
      // Expected exception, test passes
      Assert.assertTrue(true);
    }
  }

  @Test
  public void incrementPatch() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    Assert.assertEquals(SemanticVersion.parse("1.2.4"), v1.incrementPatch());
  }

  @Test
  public void incrementMinor() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    Assert.assertEquals(SemanticVersion.parse("1.3.0"), v1.incrementMinor());
  }

  @Test
  public void incrementMajor() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    Assert.assertEquals(SemanticVersion.parse("2.0.0"), v1.incrementMajor());
  }

  @Test
  public void testIncrement() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    Assert.assertEquals(SemanticVersion.parse("1.2.4"), v1.increment(SemanticVersion.INCREMENT_LEVEL.PATCH));
    Assert.assertEquals(SemanticVersion.parse("1.3.0"), v1.increment(SemanticVersion.INCREMENT_LEVEL.MINOR));
    Assert.assertEquals(SemanticVersion.parse("2.0.0"), v1.increment(SemanticVersion.INCREMENT_LEVEL.MAJOR));
    try {
      v1.increment(null);
      Assert.fail("Expected NullPointerException for null increment level");
    } catch (NullPointerException e) {
      // Expected exception, test passes
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testEquals() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    SemanticVersion v2 = SemanticVersion.parse("1.2.3");
    SemanticVersion v3 = SemanticVersion.parse("1.2.4");
    SemanticVersion v4 = SemanticVersion.parse("1.3.3");
    SemanticVersion v5 = SemanticVersion.parse("2.2.3");
    Assert.assertEquals(v1, v1);
    Assert.assertEquals(v1, v2);
    Assert.assertNotEquals(v1, v3);
    Assert.assertNotEquals(v1, v4);
    Assert.assertNotEquals(v1, v5);
    Assert.assertNotEquals(v2, v3);
    Assert.assertNotEquals(v2, v4);
    Assert.assertNotEquals(v2, v5);
    Assert.assertNotEquals(v1, v4);
    Assert.assertNotEquals(v1, v5);

    Assert.assertNotEquals(v1, "1.2.3");
  }

  @Test
  public void testHashCode() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    SemanticVersion v2 = SemanticVersion.parse("1.2.3");
    SemanticVersion v3 = SemanticVersion.parse("1.2.4");
    Assert.assertEquals(v1.hashCode(), v2.hashCode());
    Assert.assertNotEquals(v1.hashCode(), v3.hashCode());
    Assert.assertNotEquals(v2.hashCode(), v3.hashCode());
  }

  @Test
  public void testToString() {
    SemanticVersion v1 = SemanticVersion.parse("1.2.3");
    Assert.assertEquals("1.2.3", v1.toString());
  }
}