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
package edu.kit.datamanager.metastore2.dao;

import edu.kit.datamanager.metastore2.domain.Resource2FileVersion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;
import java.util.Optional;

/**
 * Database linking version of a data resource to (file)version of the file.
 */
public interface IResource2FileVersionDao extends JpaRepository<Resource2FileVersion, String>, JpaSpecificationExecutor<Resource2FileVersion> {
  Optional<Resource2FileVersion> findByResourceIdAndVersion(String resourceId, String version);
  List<Resource2FileVersion> findByResourceIdOrderByVersionDesc(String resourceId);
  Optional<Resource2FileVersion> findFirstByResourceIdOrderByVersionDesc(String resourceId);
}
