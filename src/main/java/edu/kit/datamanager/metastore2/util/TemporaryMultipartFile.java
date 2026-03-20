package edu.kit.datamanager.metastore2.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class TemporaryMultipartFile implements MultipartFile, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TemporaryMultipartFile.class);

  private final String name;
  private final String originalFilename;
  private final String contentType;
  private final File tempFile;
  public TemporaryMultipartFile(String name, String originalFilename, InputStream inputStream) throws IOException {
    this.name = name;
    this.originalFilename = originalFilename;
    contentType = determineContentType(originalFilename);
    this.tempFile = File.createTempFile(originalFilename, ".tmp");
    this.tempFile.deleteOnExit();
    Files.copy(inputStream, this.tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }
  @Override
  public String getName() {
    return name;
  }
  @Override
  public String getOriginalFilename() {
    return originalFilename;
  }
  @Override
  public String getContentType() {
    return contentType;
  }
  @Override
  public InputStream getInputStream() throws IOException {
    return new FileInputStream(tempFile);
  }
  @Override
  public long getSize() {
    return tempFile.length();
  }
  @Override
  public boolean isEmpty() {
    return tempFile.length() == 0;
  }
  @Override
  public byte[] getBytes() throws IOException {
    return Files.readAllBytes(tempFile.toPath());
  }
  @Override
  public void transferTo(File dest) throws IOException, IllegalStateException {
    Files.copy(tempFile.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }
  private String  determineContentType(String fileName) {
    String contentType = null;
    if (fileName != null) {
      if (fileName.endsWith(".json")) {
        contentType = MediaType.APPLICATION_JSON_VALUE;
      } else if (fileName.endsWith(".xml") || fileName.endsWith(".xsd")) {
        contentType = MediaType.APPLICATION_XML_VALUE;
      }
    }
    return contentType;
  }
  @Override
  public void close() throws IOException {
    LOG.warn("Try to delete temporary file '{}'!", tempFile.getName());
    if (tempFile.exists()) {
      LOG.warn("Temporary file '{}' will be deleted!", tempFile.getName());
      boolean delete = tempFile.delete();
      if (!delete) {
        LOG.error("Temporary file '{}' cannot be deleted!", tempFile.getName());
      }
    }
  }
}
