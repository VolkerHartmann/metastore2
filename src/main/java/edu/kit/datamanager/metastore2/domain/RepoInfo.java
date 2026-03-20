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
package edu.kit.datamanager.metastore2.domain;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Entity
@Data
@Table(uniqueConstraints = {
        @UniqueConstraint(columnNames = {"organization", "repoName", "path2Schema"})})
public class RepoInfo {
  @Id
  @NotBlank(message = "Unique id for schema. Pattern [a-z_-]+")
  String schemaId;
  @NotBlank(message = "Organization or user that owns the repository.")
  String organization;
  @NotBlank(message = "Name of the repository (without extension '.git').")
  String repoName;
  @NotNull(message = "The relative path to the schema file. This is used to locate the schema file in the repository. It is expected to be a valid path string, e.g., 'schemas/schema.json'.")
  String path2Schema;
  @Schema(
          description = "The tag name of the latest release. This is used to identify the version of the schema. It is expected to be in the format 'vX.Y.Z' where X, Y, and Z are integers.",
          example = "0.9.0"
  )
  String tagName;
  @Schema(
          description = "The eTag of the latest release. This is used to avoid unnecessary traffic when checking for updates. It is expected to be a string returned by the GitHub API."
  )
  String eTag;

  protected RepoInfo() {
  }
  /**
   * Constructor for RepoInfo.
   *
   * @param organization the organization or user that owns the repository
   * @param repoName the name of the repository (without extension '.git')
   * @param path2Schema the relative path to the schema file.
   */
  public RepoInfo(String organization, String repoName, String path2Schema) {
    this.organization = organization;
    this.repoName = repoName;
    this.path2Schema = path2Schema;
  }

  /**
   * Get the organization or user that owns the repository.
   * @return the organization or user name
   */
  public String getOrganization() {
    return organization;
  }
  /**
   * Set the organization or user that owns the repository.
   * @param organization the organization or user name to set
   */
  public void setOrganization(String organization) {
    this.organization = organization;
  }
  /**
   * Get the name of the repository.
   * @return the repository name (without '.git')
   */
  public String getRepoName() {
    return repoName;
  }
  /**
   * Set the name of the repository.
   * @param repoName the repository name to set (without '.git')
   */
  public void setRepoName(String repoName) {
    this.repoName = repoName;
  }
  /**
   * Get the schemaId of the schema.
   * @return the schemaId of the schema
   */
  public String getSchemaId() {
    return schemaId;
  }
  /**
   * Set the schemaId of the schema.
   * @param schemaId the schemaId of the schema
   */
  public void setSchemaId(String schemaId) {
    this.schemaId = schemaId;
  }

  /**
   * Get the tag name of the latest release.
   * @return the tag name of the latest release
   */
  public String getTagName() {
    return tagName;
  }

  /**
   * Set the tag name of the latest release.
   * @param tagName the tag name of the latest release
   */
  public void setTagName(String tagName) {
    this.tagName = tagName;
  }

  /**
   * Get the eTag of the latest release.
   * This could be used to avoid unnecessary traffic.
   * @return eTag of the latest release
   */
  public String getETag() {
    return eTag;
  }

  /**
   * Set the eTag of the latest release.
   * @param eTag eTag of the latest release.
   */
  public void setETag(String eTag) {
    this.eTag = eTag;
  }
  public String getVersion()  {
    return tagName != null ? tagName.substring(tagName.lastIndexOf('v') + 1) : "0.0.1";
  }

  /**
   * Set the path to the schema file in the repository.
   * @param path2Schema path to the schema.
   */
  public void setPath2Schema(String path2Schema) {
    this.path2Schema = path2Schema;
  }

  /**
   * Get the path to the schema in the repository.
   * @return the path to the schema in the repository
   */
  public String getPath2Schema() {
    return path2Schema;
  }
  /**
   * Override the toString method to provide a string representation of the RepoInfo object.
   * @return  a string representation of the RepoInfo object
   */
  @Override
  public String toString(){
    return "RepoInfo{" +
            ", schemaId='" + schemaId + "'" +
            ", organization='" + organization + "'" +
            ", repoName='" + repoName + "'" +
            ", path2Schema='" + path2Schema + "'" +
            ", tagName='" + tagName + "'" +
            ", eTag='" + eTag + "' }";
  }
}
