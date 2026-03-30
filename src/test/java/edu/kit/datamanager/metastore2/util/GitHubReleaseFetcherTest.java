package edu.kit.datamanager.metastore2.util;

import edu.kit.datamanager.metastore2.domain.RepoInfo;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Files;

public class GitHubReleaseFetcherTest {

  @Test
  public void testGetSchemaId_nullOrInvalid() {
    Assert.assertNull(GitHubReleaseFetcher.getSchemaId(null));
    Assert.assertNull(GitHubReleaseFetcher.getSchemaId("nosplit"));
  }

  @Test
  public void testGetSchemaId_valid() {
    String tag = "my_schema_v1.2.3";
    String schemaId = GitHubReleaseFetcher.getSchemaId(tag);
    Assert.assertEquals("my_schema", schemaId);
  }

  @Test
  public void testGetVersionFromTag() {
    Assert.assertEquals("", GitHubReleaseFetcher.getVersionFromTag(null));
    Assert.assertEquals("", GitHubReleaseFetcher.getVersionFromTag("no_v_here"));
    Assert.assertEquals("", GitHubReleaseFetcher.getVersionFromTag("s_v2.0"));
    Assert.assertEquals("", GitHubReleaseFetcher.getVersionFromTag("s_v2.0.0_extra"));
    Assert.assertEquals("1.2.3", GitHubReleaseFetcher.getVersionFromTag("schema_v1.2.3"));
  }

  @Test
  public void testGetLatestJsonSchema_fileUrl() throws Exception {
    byte[] content = "{\"hello\":\"world\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    File temp = File.createTempFile("github-release-fetcher-test", ".json");
    temp.deleteOnExit();
    Files.write(temp.toPath(), content);

    String fileUrl = temp.toURI().toString();
    MultipartFile mf = GitHubReleaseFetcher.getLatestJsonSchema(fileUrl);
    Assert.assertNotNull("MultipartFile should not be null for an existing file URL", mf);
    Assert.assertEquals(temp.getName(), mf.getOriginalFilename());
    Assert.assertArrayEquals(content, mf.getBytes());
    // try to close if implementation supports AutoCloseable
    try {
      if (mf instanceof AutoCloseable) {
        ((AutoCloseable) mf).close();
      }
    } catch (Exception ex) {
      // ignore close exceptions in test cleanup
    }
  }

  @Test
  public void testFetchLatestReleaseWithGitHubAction() throws Exception {
    RepoInfo repoInfo = new RepoInfo("kit-data-manager",
            "metadata-schemas-for-materials-science",
            "not needed");
    repoInfo.setSchemaId("creep-test");
    MultipartFile schemaFile = GitHubReleaseFetcher.fetchLatestRelease(repoInfo);
    Assert.assertNotNull(schemaFile);
    Assert.assertTrue(schemaFile.getSize() > 0);
    Assert.assertEquals(MediaType.APPLICATION_JSON_VALUE, schemaFile.getContentType());
    Assert.assertEquals("schema", schemaFile.getName());
    Assert.assertEquals("bundled-CREEP-TEST_v1.1.0.json", schemaFile.getOriginalFilename());

    repoInfo.setSchemaId("unknown");
      schemaFile = GitHubReleaseFetcher.fetchLatestRelease(repoInfo);
      Assert.assertNull(schemaFile);
  }

  @Test
  public void testFetchLatestReleaseWithUnknownSchema() throws Exception {
    RepoInfo repoInfo = new RepoInfo("kit-data-manager",
            "service-base",
            "not needed");
    repoInfo.setSchemaId("unknown");
      MultipartFile schemaFile = GitHubReleaseFetcher.fetchLatestRelease(repoInfo);
      Assert.assertNull(schemaFile);
  }

  @Test
  public void testFetchLatestRelease() throws Exception {
    RepoInfo repoInfo = new RepoInfo("kit-data-manager",
            "metastore2",
            "src/test/resources/examples/xml/kit.xsd");
    repoInfo.setSchemaId("notRelevant");
    MultipartFile schemaFile = GitHubReleaseFetcher.fetchLatestRelease(repoInfo);
    Assert.assertNotNull(schemaFile);
    Assert.assertTrue(schemaFile.getSize() > 0);
    Assert.assertEquals(MediaType.APPLICATION_XML_VALUE, schemaFile.getContentType());
    Assert.assertEquals("schema", schemaFile.getName());
    Assert.assertEquals("kit.xsd", schemaFile.getOriginalFilename());
  }

  @Test
  public void testFetchLatestReleaseWithWrongPath() throws Exception {
    RepoInfo repoInfo = new RepoInfo("kit-data-manager",
            "metastore2",
            "src/test/java/resources/invalid/kit.xsd");
    repoInfo.setSchemaId("notRelevant");
    MultipartFile schemaFile = GitHubReleaseFetcher.fetchLatestRelease(repoInfo);
    Assert.assertNull(schemaFile);
  }

}

