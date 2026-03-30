package edu.kit.datamanager.metastore2.domain;

import org.junit.Assert;
import org.junit.Test;

public class RepoInfoTest {

  @Test
  public void testConstructorAndBasicGetters() {
    RepoInfo ri = new RepoInfo("orgName", "repoName", "schemas/schema.json");
    Assert.assertEquals("orgName", ri.getOrganization());
    Assert.assertEquals("repoName", ri.getRepoName());
    Assert.assertEquals("schemas/schema.json", ri.getPath2Schema());
  }

  @Test
  public void testSettersAndGetters() {
    RepoInfo ri = new RepoInfo("o", "r", "p");
    ri.setSchemaId("my_schema");
    ri.setReleaseName("v1.2.3");
    ri.setTagName("tag");
    ri.setETag("etag-value");

    Assert.assertEquals("my_schema", ri.getSchemaId());
    Assert.assertEquals("v1.2.3", ri.getReleaseName());
    Assert.assertEquals("tag", ri.getTagName());
    Assert.assertEquals("etag-value", ri.getETag());
    Assert.assertEquals("p", ri.getPath2Schema());

    ri.setOrganization("orgName");
    ri.setRepoName("repoName");
    ri.setPath2Schema("path/to/schema.json");
    Assert.assertEquals("orgName", ri.getOrganization());
    Assert.assertEquals("repoName", ri.getRepoName());
    Assert.assertEquals("path/to/schema.json", ri.getPath2Schema());
  }

  @Test
  public void testGetVersion_whenTagNameNull() {
    RepoInfo ri = new RepoInfo("o", "r", "p");
    ri.setTagName(null);
    Assert.assertEquals("0.0.0", ri.getVersion());
  }

  @Test
  public void testGetVersion_withVPrefixAndOtherFormats() {
    RepoInfo ri = new RepoInfo("o", "r", "p");
    ri.setTagName("v1.2.3");
    Assert.assertEquals("1.2.3", ri.getVersion());

    ri.setTagName("release_v2.0.0");
    Assert.assertEquals("2.0.0", ri.getVersion());

    // when no 'v' is present, entire tag will be returned (per implementation)
    ri.setTagName("release-2025");
    Assert.assertEquals("release-2025", ri.getVersion());

    // tag ends with 'v' -> empty version
    ri.setTagName("prefix_v");
    Assert.assertEquals("", ri.getVersion());
  }

  @Test
  public void testToStringContainsFields() {
    RepoInfo ri = new RepoInfo("org", "repo", "p");
    ri.setSchemaId("id1");
    ri.setReleaseName("v0.0.1");
    ri.setTagName("tag1");
    ri.setETag("0001394");
    String s = ri.toString();
    Assert.assertTrue(s.contains("schemaId"));
    Assert.assertTrue(s.contains("id1"));
    Assert.assertTrue(s.contains("org"));
    Assert.assertTrue(s.contains("repo"));
    Assert.assertTrue(s.contains("v0.0.1"));
    Assert.assertTrue(s.contains("tag1"));
    Assert.assertTrue(s.contains("0001394"));
  }

}

