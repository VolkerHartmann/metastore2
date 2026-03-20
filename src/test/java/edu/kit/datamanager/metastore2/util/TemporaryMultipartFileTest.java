package edu.kit.datamanager.metastore2.util;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class TemporaryMultipartFileTest {

  @Test
  public void testConstructorAndGetters4JsonFile() throws Exception {
    byte[] data = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "test.json", new ByteArrayInputStream(data))) {
      Assert.assertEquals("p", tmf.getName());
      Assert.assertEquals("test.json", tmf.getOriginalFilename());
      Assert.assertEquals(MediaType.APPLICATION_JSON_VALUE, tmf.getContentType());
      Assert.assertEquals(data.length, tmf.getSize());
      Assert.assertFalse(tmf.isEmpty());
      Assert.assertArrayEquals(data, tmf.getBytes());
      try (InputStream in = tmf.getInputStream()) {
        byte[] read = in.readAllBytes();
        Assert.assertArrayEquals(data, read);
      }
    }
  }
  @Test
  public void testConstructorAndGetters4XmlFile() throws Exception {
    byte[] data = "<a>1<\\a>".getBytes(StandardCharsets.UTF_8);
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "test.json", new ByteArrayInputStream(data))) {
      Assert.assertEquals("p", tmf.getName());
      Assert.assertEquals("test.json", tmf.getOriginalFilename());
      Assert.assertEquals(MediaType.APPLICATION_JSON_VALUE, tmf.getContentType());
      Assert.assertEquals(data.length, tmf.getSize());
      Assert.assertFalse(tmf.isEmpty());
      Assert.assertArrayEquals(data, tmf.getBytes());
      try (InputStream in = tmf.getInputStream()) {
        byte[] read = in.readAllBytes();
        Assert.assertArrayEquals(data, read);
      }
    }
  }
  @Test
  public void testGetInputStreamMultipleTimes() throws Exception {
    byte[] data = "{\"a\":1}".getBytes( StandardCharsets.UTF_8);
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "test.json", new ByteArrayInputStream(data))) {
      try (InputStream in1 = tmf.getInputStream(); InputStream in2 = tmf.getInputStream()) {
        byte[] read1 = in1.readAllBytes();
        byte[] read2 = in2.readAllBytes();
        Assert.assertArrayEquals(data, read1);
        Assert.assertArrayEquals(data, read2);
      }
      try (InputStream in = tmf.getInputStream()) {
      byte[] read1 = in.readAllBytes();
        byte[] read2 = in.readAllBytes();
        Assert.assertArrayEquals(data, read1);
        Assert.assertEquals(0, read2.length);
      }
    }
  }

  @Test
  public void testGetBytesMultipleTimes() throws Exception {
    byte[] data = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "test.json", new ByteArrayInputStream(data))) {
      byte[] bytes1 = tmf.getBytes();
      byte[] bytes2 = tmf.getBytes();
      Assert.assertArrayEquals(data, bytes1);
      Assert.assertArrayEquals(data, bytes2);
    }
  }

  @Test
  public void testContentType_xml_and_xsd() throws Exception {
    byte[] data = "<a></a>".getBytes(StandardCharsets.UTF_8);
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "test.xml", new ByteArrayInputStream(data))) {
      Assert.assertEquals(MediaType.APPLICATION_XML_VALUE, tmf.getContentType());
    }
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "schema.xsd", new ByteArrayInputStream(data))) {
      Assert.assertEquals(MediaType.APPLICATION_XML_VALUE, tmf.getContentType());
    }
  }

  @Test
  public void testUnknownContentType() throws Exception {
    byte[] data = "plain".getBytes(StandardCharsets.UTF_8);
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "file.txt", new ByteArrayInputStream(data))) {
      Assert.assertNull(tmf.getContentType());
    }
  }

  @Test
  public void testTransferTo() throws Exception {
    byte[] data = "transfer content".getBytes(StandardCharsets.UTF_8);
    File dest = File.createTempFile("dest", ".tmp");
    dest.deleteOnExit();
    try (TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "file.txt", new ByteArrayInputStream(data))) {
      tmf.transferTo(dest);
      byte[] read = Files.readAllBytes(dest.toPath());
      Assert.assertArrayEquals(data, read);
    }

  }

  @Test
  public void testCloseDeletesTempFile() throws Exception {
    byte[] data = "to delete".getBytes(StandardCharsets.UTF_8);
    TemporaryMultipartFile tmf = new TemporaryMultipartFile("p", "file.txt", new ByteArrayInputStream(data));
    java.lang.reflect.Field f = TemporaryMultipartFile.class.getDeclaredField("tempFile");
    f.setAccessible(true);
    File temp = (File) f.get(tmf);
    Assert.assertTrue("temp file should exist before close", temp.exists());
    tmf.close();
    Assert.assertFalse("temp file should be deleted after close", temp.exists());
    try {
      tmf.close();
    } catch (Exception ex) {
      // Should not happen
      Assert.assertTrue(ex.getMessage(), false);
    }

  }

}

