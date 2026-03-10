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
package edu.kit.datamanager.metastore2.service;

import edu.kit.datamanager.metastore2.util.DataResourceRecordUtil;
import edu.kit.datamanager.repo.domain.DataResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service for indexing data resources in ElasticSearch.
 * This service is responsible for querying data resources and their related identifiers, which can be used for indexing in ElasticSearch.
 */
@Component
public class ElasticIndexerService {

  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexerService.class);

  /**
   * Constructor.
   */
  public ElasticIndexerService() {
  }

  /**
   * Initialize and update the metrics for the metastore in regular manner.
   */
  @Transactional(readOnly = true)
  public Page<DataResource> queryDataResourcesWithRelatedIdentifiers(Specification<DataResource> spec, Pageable pgbl) {
    Page<DataResource> records = null;
    try {
      records = DataResourceRecordUtil.queryDataResources(spec, pgbl);
    } catch (Exception e) {
      LOG.error("Error querying data resources: {}", e.getMessage(), e);
    }
    // Force dataresource to load related identifiers
    records.getContent().forEach(record -> {
      DataResourceRecordUtil.getSchemaIdentifier(record);
//      LOG.debug("Queried data resource: {}", record.getId());
//      LOG.trace("Queried data resource: {}", record.getRelatedIdentifiers());
    });
    return records;
  }
}
