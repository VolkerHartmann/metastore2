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
package edu.kit.datamanager.metastore2.service;

import edu.kit.datamanager.entities.RepoUserRole;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.IRepoInfoDao;
import edu.kit.datamanager.metastore2.domain.RepoInfo;
import edu.kit.datamanager.metastore2.util.DataResourceRecordUtil;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.repo.util.DataResourceUtils;
import io.micrometer.common.lang.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class SchemaRegistryService {

  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryService.class);

  private final MetastoreConfiguration schemaConfig;

  private final IRepoInfoDao repoInfoDao;

  /**
   * Constructor.
   *
   * @param repoInfoDao DAO for repoInfo.
   */
  public SchemaRegistryService(@NonNull MetastoreConfiguration schemaConfig,
                               @org.springframework.lang.NonNull IRepoInfoDao repoInfoDao) {
    this.schemaConfig = schemaConfig;
    this.repoInfoDao = repoInfoDao;
  }

  /**
   * Update registered schemas if a new version is available.
   */
  @Transactional
  public void updateRegisteredSchemas() {
    if (repoInfoDao.count() > 0) {
      // Set context to ADMINISTRATOR
      Authentication auth = new UsernamePasswordAuthenticationToken("admin", "admin", List.of(new SimpleGrantedAuthority(RepoUserRole.ADMINISTRATOR.toString())));
      SecurityContext context = SecurityContextHolder.createEmptyContext();
      context.setAuthentication(auth);
      SecurityContextHolder.setContext(context);
      try {
        LOG.info("Start schema registration scheduler trying to update '{}' schema(s)!", repoInfoDao.count());
        // Check all registered schemas...
        for (RepoInfo repoInfo : repoInfoDao.findAll()) {
          LOG.trace("Checking the following schema: {}", repoInfo);
          DataResource schemaDocument;
          try {
            DataResource dataResource = DataResourceRecordUtil.getSchemaRecordByIdAndVersion(schemaConfig, repoInfo.getSchemaId(), repoInfo.getVersion());
            dataResource = DataResourceUtils.copyDataResource(dataResource);
            schemaDocument = DataResourceRecordUtil.createOrUpdateDataResourceRecord4GitHubSchema(schemaConfig, dataResource, repoInfo);
            if (schemaDocument != null) {
              // Update RepoInfo
              LOG.trace("RepoInfo {} has been updated", repoInfo);
            } else {
              LOG.trace("No updates available for {}", repoInfo);
            }
          } catch (Exception e) {
            LOG.error("Error reading schema for RepoInfo: {}", repoInfo);
            LOG.error(e.getMessage(), e);
          }
        }
      } finally {
        // Clear context
        SecurityContextHolder.clearContext();
      }
    } else {
      LOG.info("No schemas registered.");
    }
  }
}
