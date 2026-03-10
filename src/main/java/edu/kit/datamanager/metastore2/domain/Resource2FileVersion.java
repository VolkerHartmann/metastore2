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
package edu.kit.datamanager.metastore2.domain;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.io.Serializable;

/**
 * Entity for holding internal version information about the linked
 * content information to a version of a given data resource.
 */
@Entity
@Data
@Table(uniqueConstraints = {
        @UniqueConstraint(columnNames = {"resourceId", "version"})})
public class Resource2FileVersion implements Serializable {
  @Id
  @GeneratedValue
  private Long id;
  @NotBlank(message = "The unique identifier of the data resource.")
  private String resourceId;
  @NotBlank(message = "Semanticversion of the data resource.")
  private String version;
  @NotNull(message = "Path of schema document linked to identifier and version.")
  private Integer fileVersion;
}
