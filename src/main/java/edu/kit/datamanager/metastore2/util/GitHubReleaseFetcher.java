/*
 * Copyright 2025 Karlsruhe Institute of Technology.
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

import com.fasterxml.jackson.databind.JsonNode;
import edu.kit.datamanager.exceptions.CustomInternalServerError;
import edu.kit.datamanager.metastore2.domain.RepoInfo;
import edu.kit.datamanager.repo.domain.DataResource;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.util.Optional;

public class GitHubReleaseFetcher {

  /**
   * Logger for messages.
   */
  private static final Logger LOG = LoggerFactory.getLogger(GitHubReleaseFetcher.class);

  private static final String PLACEHOLDER_ORGANIZATION = "{{organization}}";
  private static final String PLACEHOLDER_REPO = "{{repo}}";
  private static final String PLACEHOLDER_TAG_NAME = "{{tagName}}";
  private static final String PLACEHOLDER_PATH2SCHEMA = "{{path2Schema}}";
  private static final String LATEST_SCHEMA_URL_TEMPLATE = "https://raw.githubusercontent.com/" + PLACEHOLDER_ORGANIZATION +
          "/" + PLACEHOLDER_REPO +
          "/" + PLACEHOLDER_TAG_NAME +
          "/" + PLACEHOLDER_PATH2SCHEMA;

  private static final String ALL_TAGS_URL_TEMPLATE = "https://api.github.com/repos/" + PLACEHOLDER_ORGANIZATION +
          "/" + PLACEHOLDER_REPO +
          "/releases";

  private GitHubReleaseFetcher() {
  }

  /**
   * Fetches the latest release information from the GitHub repository.
   * @param repoInfo RepoInfo object containing the organization, repository name, and path2Schema for which to fetch the latest release.
   *
   * @return MultipartFile containing the latest release's schema document, or null if no matching release is found or an error occurs.
   * @throws IOException if an I/O error occurs while fetching the latest release information.
   */
  public static MultipartFile fetchLatestRelease(RepoInfo repoInfo) throws IOException {
    MultipartFile schemaDocument = null;
    String apiUrl = ALL_TAGS_URL_TEMPLATE
            .replace(PLACEHOLDER_ORGANIZATION, repoInfo.getOrganization())
            .replace(PLACEHOLDER_REPO, repoInfo.getRepoName());
    String downloadUrl4Schema;
    Optional<SemanticVersion> currentVersionOptional = SemanticVersion.tryParse(repoInfo.getVersion());
    SemanticVersion currentVersion = null;
    // Set current version to '0.0.0' if no further version is available.
    currentVersion = currentVersionOptional.orElseGet(() -> SemanticVersion.parse("0.0.0"));

    try {
      URL url = new URL(apiUrl);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/vnd.github.v3+json");
      if (repoInfo.getETag() != null) {
        conn.setRequestProperty("If-None-Match", repoInfo.getETag());
      }

      if (conn.getResponseCode() == 200) {
        // Fetch eTag to avoid unnecessary traffic
        repoInfo.setETag(conn.getHeaderField("etag"));
        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
        StringBuilder response = new StringBuilder();
        String output;
        while ((output = br.readLine()) != null) {
          response.append(output);
        }

        conn.disconnect();
        LOG.trace("Fetching lastest release from GitHub repository: {}/{}", repoInfo.getOrganization(), repoInfo.getRepoName());
        JsonNode jsonNode = JsonUtils.getJsonNodeFromString(response.toString());
        for (JsonNode node : jsonNode) {
          // Get release name. The release name consists of the schemaId and the version number. The version number is determined by the part after the last '_' in the release name prefixed by a 'v'.
          String name = node.path("name").asText("No release yet!");
          String tag = node.path("tag_name").asText(null);
          LOG.trace("Found release: '{}' (tag: '{}')", name, tag);
          // Extract the part before the last '_' of tag to determine schemaId
          String schemaId = getSchemaId(tag);
          // Extract the part after the last 'v'
          String newVersion = getVersionFromTag(tag);
          LOG.trace("New release version: '{}'", newVersion);
          LOG.trace("Current version: '{}'", currentVersion);
          Optional<SemanticVersion> newVersionOptional = SemanticVersion.tryParse(newVersion);
          if (newVersionOptional.isEmpty()) {
            // Skip that tag due to missing version information. This can be the case if the tag name does not follow the expected format (e.g. schemaId_v1.2.3).
            LOG.debug("Can't parse version from tag: '{}'. Skip this tag and continue with next tag...", tag);
            continue;
          }
          if (((schemaId == null) || schemaId.equals(repoInfo.getSchemaId())) && newVersionOptional.get().isAfter(currentVersion)) {
            LOG.debug("Matching release found: " + name);
            // Set release name
            repoInfo.setReleaseName(name);
            // Set tag
            repoInfo.setTagName(tag);
            // Set version
            LOG.debug("Extracted version: " + newVersion);
            // if an asset is available by a GitHub action this will be downloaded. The download URL is determined by the first asset in the release.
            // If no asset is available, try to download the file from the given path.
            downloadUrl4Schema = node.path("assets").path(0).path("browser_download_url").asText(null);
            if (downloadUrl4Schema == null) {
              // Download file from given Path for determined version.
              downloadUrl4Schema = LATEST_SCHEMA_URL_TEMPLATE
                      .replace(PLACEHOLDER_ORGANIZATION, repoInfo.getOrganization())
                      .replace(PLACEHOLDER_REPO, repoInfo.getRepoName())
                      .replace(PLACEHOLDER_TAG_NAME, repoInfo.getTagName())
                      .replace(PLACEHOLDER_PATH2SCHEMA, repoInfo.getPath2Schema());
            }
            schemaDocument = getLatestJsonSchema(downloadUrl4Schema);
            break;
          } else {
            LOG.debug("Release " + name + " does not match the expected schemaId: " + repoInfo.getSchemaId());
          }
        }
      }
      if (schemaDocument == null) {
        LOG.debug("No new version available from GitHub repository: '{}/{}'", repoInfo.getOrganization(), repoInfo.getRepoName());
      }

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new CustomInternalServerError("Error fetching latest release information from GitHub repository: " + repoInfo.getOrganization() + "/" + repoInfo.getRepoName());
    }
    return schemaDocument;
  }

  public static MultipartFile getLatestJsonSchema(String url4Schema) {

    TemporaryMultipartFile multipartFile = null;
    String originalFilename = FilenameUtils.getName(url4Schema);

    try (InputStream inputStream = new URL(url4Schema).openStream()) {
      multipartFile = new TemporaryMultipartFile("schema", originalFilename, inputStream);
    } catch (Exception e) {
      LOG.error("Error fetching JSON schema from URL: {}", url4Schema, e);
    }
    LOG.debug("Downloaded JSON schema from URL: {} with file name: {}", url4Schema, originalFilename);
    return multipartFile;
  }

  /**
   * Get schemaId from tag name.
   * The schemaId is determined by extracting the part of the tag name before the last underscore ('_').
   * e.g. schema_Id_v1.2.3 -> schemaId = schema_Id
   * If no schemaId is available an empty string is returned.
   * @param tag tag name.
   * @return schemaId
   */
  public static String getSchemaId(String tag) {
    String schemaId = null;
    if ((tag != null) && !tag.trim().isEmpty() && tag.lastIndexOf('_') > 0) {
      schemaId = tag.substring(0, tag.lastIndexOf('_'));
      DataResource dataResource = new DataResource();
      dataResource.setId(schemaId);
      DataResourceRecordUtil.check4validSchemaId(dataResource);
      schemaId = dataResource.getId();
    }
    return schemaId;
  }

  /**
   * Get version from tag name.
   * The version is determined by extracting the part of the tag name after the last underscore ('_') without the 'v' prefix.
   * e.g. schema_Id_v1.2.3 -> version = 1.2.3
   * If no valid version is available an empty string is returned.
   * @param tag tag name.
   * @return version
   */
  public static String getVersionFromTag(String tag) {
    String version = "";
      Optional<SemanticVersion> semanticVersion = getSemanticVersionFromTag(tag);
      if (semanticVersion.isPresent()) {
        version = semanticVersion.get().toString();
      }
    return version;
  }

  /**
   * Get version from tag name.
   * The version is determined by extracting the part of the tag name after the last underscore ('_') without the 'v' prefix.
   * e.g. schema_Id_v1.2.3 -> version = 1.2.3
   * If no valid version is available an empty string is returned.
   * @param tag tag name.
   * @return version
   */
  public static Optional<SemanticVersion> getSemanticVersionFromTag(String tag) {
    Optional<SemanticVersion> semanticVersion = Optional.empty();
    if ((tag != null) && !tag.trim().isEmpty() && tag.lastIndexOf('v') >= 0) {
      String expectedVersion = tag.substring(tag.lastIndexOf('v') + 1);
      semanticVersion = SemanticVersion.tryParse(expectedVersion);
    }
    return semanticVersion;
  }
}
