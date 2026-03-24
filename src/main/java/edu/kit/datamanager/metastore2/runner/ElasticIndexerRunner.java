/*
 * Copyright 2022 Karlsruhe Institute of Technology.
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
package edu.kit.datamanager.metastore2.runner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import edu.kit.datamanager.clients.SimpleServiceClient;
import edu.kit.datamanager.configuration.SearchConfiguration;
import edu.kit.datamanager.entities.messaging.MetadataResourceMessage;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.ISchemaUrl2PathDao;
import edu.kit.datamanager.metastore2.domain.SchemaUrl2Path;
import edu.kit.datamanager.metastore2.service.ElasticIndexerService;
import edu.kit.datamanager.metastore2.util.DataResourceRecordUtil;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.service.IMessagingService;
import edu.kit.datamanager.service.impl.LogfileMessagingService;
import edu.kit.datamanager.util.ControllerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import java.util.*;

/**
 * This class contains 3 runners:
 * <ul><li>Runner for indexing all metadata documents of given schemas Arguments
 * have to start with at least 'reindex' followed by all indices which have to
 * be reindexed. If no indices are given all indices will be reindexed.</li>
 * <li>Runner for migrating dataresources from version 1 to version2.</li>
 * <li>Runner for purging schema/metadata documents and it's linked database
 * entries.</li>
 * </ul>
 */
@Component
public class ElasticIndexerRunner implements CommandLineRunner {

  @Autowired
  private ElasticIndexerService elasticIndexerService;

  /*
   * ***************************************************************************
   * Parameter for migrating MetaStore version 1.x to version 2.x This should be
   * executed only once.
   * ***************************************************************************
   */
  /**
   * Start migration to version 2
   */
  @Parameter(names = {"--migrate2DataCite"}, description = "Migrate database from version 1.X to 2.X.")
  boolean doMigration2DataCite;
  /**
   * Start migration to version 2
   */
  @Parameter(names = {"--prefixIndices", "-p"}, description = "Parameter for 'migrate2Datacite': Prefix used for the indices inside elastic.")
  String prefixIndices;

  /*
   * ***************************************************************************
   * Parameters for reindexing elasticsearch. This should be executed only once.
   * ***************************************************************************
   */
  /**
   * Start reindexing...
   */
  @Parameter(names = {"--reindex"}, description = "Elasticsearch index should be build from existing documents.")
  boolean updateIndex;
  /**
   * Restrict reindexing to provided indices only.
   */
  @Parameter(names = {"--indices", "-i"}, description = "Parameter for 'reindex': Only for given indices (comma separated) or all indices if not present.")
  Set<String> indices;
  /**
   * Restrict reindexing to dataresources newer than given date.
   */
  @Parameter(names = {"--updateDate", "-u"}, description = "Parameter for 'reindex': Starting reindexing only for documents updated at earliest on update date.")
  Date updateDate;

  /*
   * ***************************************************************************
   * Parameter for purging database and disc.
   * ***************************************************************************
   */
  /**
   * Start purging databases and disc from resources with state 'GONE'
   */
  @Parameter(names = {"--purgeRepo"}, description = "Remove resources with state 'GONE'"
          + " from database and from disc.")
  boolean doPurgeRepo;
  /**
   * Start migration to version 2
   */
  @Parameter(names = {"--removeId", "-r"}, description = "Parameter for 'purgeRepo': Remove given ids (comma separated list not supported yet). "
          + "'all' will remove all resources with state 'GONE'.")
  Set<String> purgeIds;

  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexerRunner.class);
  /**
   * DAO for linking URLS to files and format.
   */
  @Autowired
  private ISchemaUrl2PathDao schemaUrl2PathDao;
  /**
   * Instance of schema repository.
   */
  @Autowired
  private MetastoreConfiguration schemaConfig;

  /**
   * Optional messagingService bean may or may not be available, depending on a
   * service's configuration. If messaging capabilities are disabled, this bean
   * should be not available. In that case, messages are only logged.
   */
  @Autowired
  private Optional<IMessagingService> messagingService;
  @Autowired
  private PurgeRunner cleanUpTool;

  @Autowired
  private SearchConfiguration searchConfiguration;

  /**
   * Start runner for actions before starting service.
   *
   * @param args Arguments for the runner.
   * @throws Exception Something went wrong.
   */
  @Override
  @SuppressWarnings({"StringSplitter", "JavaUtilDate"})
  public void run(String... args) throws Exception {
    // Set defaults for cli arguments.
    updateIndex = false;
    prefixIndices = "metastore-";
    updateDate = new Date(0);
    indices = new HashSet<>();
    doMigration2DataCite = false;
    doPurgeRepo = false;
    purgeIds = new HashSet<>();

    JCommander argueParser = JCommander.newBuilder()
            .addObject(this)
            .build();
    try {
      LOG.trace("Parse arguments: '{}'", (Object) args);
      argueParser.parse(args);
      LOG.trace("doMigration2DataCite: '{}'", doMigration2DataCite);
      LOG.trace("PrefixIndices: '{}'", prefixIndices);
      LOG.trace("Update index: '{}'", updateIndex);
      LOG.trace("update date: '{}'", updateDate.toString());
      LOG.trace("indices: '{}'", indices);
      LOG.trace("doPurgeRepo: '{}'", doPurgeRepo);
      LOG.trace("remove IDs: '{}'", purgeIds);
      LOG.trace("Find all schemas...");
      // Try to determine baseUrl 
      List<SchemaUrl2Path> findAllSchemas = schemaUrl2PathDao.findAll(PageRequest.of(0, 1)).getContent();
      if (!findAllSchemas.isEmpty()) {
        // There is at least one schema.
        // Try to fetch baseURL from this
        if (LOG.isTraceEnabled()) {
          for (SchemaUrl2Path item : findAllSchemas) {
            LOG.trace("SchemaUrl2Path: '{}'", item);
          }
        }
        SchemaUrl2Path findByPath = findAllSchemas.get(0);
        String baseUrl = findByPath.getUrl().split("/api/v1/schema")[0];
        LOG.trace("Found baseUrl: '{}'", baseUrl);
        DataResourceRecordUtil.setBaseUrl(baseUrl);
      }
      if (updateIndex) {
        updateElasticsearchIndex();
      }
      if (doPurgeRepo) {
        cleanUpTool.removeResources(purgeIds);
      }
    } catch (Exception ex) {
      LOG.error("Error while executing runner!", ex);
      argueParser.usage();
      System.exit(0);
    }
  }

  /**
   * Start runner to reindex dataresources according to the given parameters.
   *
   * @throws InterruptedException Something went wrong.
   */
  private void updateElasticsearchIndex() throws InterruptedException {
    LOG.info("Start ElasticIndexer Runner for indices '{}' and update date '{}'", indices, updateDate);
    LOG.info("No of schemas: '{}'", DataResourceRecordUtil.getNoOfSchemaDocuments());
    determineIndices(indices);
    for (String index : indices) {
      LOG.info("Reindex '{}'", index);
      Specification<DataResource> specification = DataResourceRecordUtil.findBySchemaId(null, Arrays.asList(index));
      specification = DataResourceRecordUtil.findByUpdateDates(specification, updateDate.toInstant(), null);
      int page = 0;
      int pageSize = 20;
      Page<DataResource> resultPage;
      do {
        LOG.debug("Performing query for records. Page: '{}'", page);
        Pageable pgbl = PageRequest.of(page, pageSize, Sort.by("lastUpdate").descending());
        resultPage = elasticIndexerService.queryDataResourcesWithRelatedIdentifiers(specification, pgbl);
        LOG.debug("Find '{}' records!", resultPage.getNumberOfElements());
        for (DataResource item : resultPage.getContent()) {
          LOG.trace("Sending CREATE event.");
          messagingService.orElse(new LogfileMessagingService()).
                  send(MetadataResourceMessage.factoryCreateMetadataMessage(item, this.getClass().toString(), ControllerUtils.getLocalHostname()));
        }
        page++;
      } while (resultPage.hasNext());
//      indexAlternativeSchemaIds(index, baseUrl);
    }
    Thread.sleep(5000);

    LOG.trace("Finished ElasticIndexerRunner!");
  }

  /**
   * Determine all indices if an empty set is provided. Otherwise, return
   * provided set without any change.
   *
   * @param indices Indices which should be reindexed.
   */
  private void determineIndices(Set<String> indices) {
    if (indices.isEmpty()) {
      LOG.info("Reindex all indices!");
      // Search for all indices...
      // Build Specification
      Specification<DataResource> spec = DataResourceRecordUtil.findByResourceType(null, DataResourceRecordUtil.SCHEMA_SUFFIX);
      // Hide revoked and gone data resources.
      spec = DataResourceRecordUtil.findByStateWithAuthorization(spec, DataResource.State.FIXED, DataResource.State.VOLATILE);
      int entriesPerPage = 20;
      int page = 0;
      LOG.debug("Performing query for records.");
      Page<DataResource> records;
      do {
        Pageable pgbl = PageRequest.of(page, entriesPerPage, Sort.by("lastUpdate").descending());
        records = DataResourceRecordUtil.queryDataResources(spec, pgbl);
        int noOfEntries = records.getNumberOfElements();
        int noOfPages = records.getTotalPages();

        LOG.debug("Page '{}' of '{}':Found '{}' schemas!", page, noOfPages, noOfEntries);
        for (DataResource schema : records.getContent()) {
          indices.add(schema.getId());
        }
        page++;
      } while (records.hasNext());
    }
  }

//  private void indexAlternativeSchemaIds(String index, String baseUrl) {
//    LOG.trace("Search for alternative schemaId (given as URL)");
//    List<SchemaUrl2Path> findSchemaBySchemaId = schemaSchemaUrl2PathDao.findBySchemaIdOrderByVersionDesc(index);
//
//    for (SchemaUrl2Path debug : findSchemaBySchemaId) {
//      templateRecord.setSchemaId(debug.getSchemaId());
//      templateRecord.setSchemaVersion(debug.getVersion());
//      List<SchemaUrl2Path> findByPath1 = schemaSchemaUrl2PathDao.findByPath(debug.getSchemaDocumentUri());
//      for (SchemaUrl2Path path : findByPath1) {
//        LOG.trace("SchemaRecord: '{}'", debug);
//        List<DataRecord> findBySchemaUrl = dataRecordDao.findBySchemaIdAndLastUpdateAfter(path.getUrl(), updateDate.toInstant());
//        LOG.trace("Search for documents for schema '{}' and update date '{}'", path.getUrl(), updateDate);
//        LOG.trace("No of documents: '{}'", findBySchemaUrl.size());
//        for (DataRecord item : findBySchemaUrl) {
//          templateRecord.setMetadataId(item.getMetadataId());
//          templateRecord.setVersion(item.getVersion());
//          MetadataRecord result = toMetadataRecord(templateRecord, baseUrl);
//          LOG.trace("Sending CREATE event (alternativeSchemaId: '{}').", index);
//          messagingService.orElse(new LogfileMessagingService()).
//                  send(MetadataResourceMessage.factoryCreateMetadataMessage(result, this.getClass().toString(), ControllerUtils.getLocalHostname()));
//        }
//      }
//    }
//
//  }

  /**
   * Remove all indexed entries (indexed with V1) for given schema. (If search
   * is enabled)
   * <p>
   * example: POST /metastore-schemaid/_delete_by_query { "query": { "range": {
   * "metadataRecord.schemaVersion": { "gte": 1 } } } }
   *
   * @param schemaId schema
   */
  private void removeAllIndexedEntries(String schemaId) {
    // Delete all entries in elastic (if available)
    LOG.trace("Remove all indexed entries for '{}'...", schemaId);
    if (searchConfiguration.isSearchEnabled()) {
      String prefix4Indices = prefixIndices;

      LOG.trace(searchConfiguration.toString());
      LOG.trace("Remove all entries for index: '{}'", prefix4Indices + schemaId);
      SimpleServiceClient client = SimpleServiceClient.create(searchConfiguration.getUrl() + "/" + prefix4Indices + schemaId + "/_delete_by_query");
      String query = "{ \"query\": { \"range\" : { \"metadataRecord.schemaVersion\" : { \"gte\" : 1} } } }";
      LOG.trace("Query: '{}'", query);
      client.withContentType(MediaType.APPLICATION_JSON);
      try {
        String postResource = client.postResource(query, String.class);
        LOG.trace(postResource);
      } catch (HttpClientErrorException hcee) {
        LOG.error(hcee.getMessage());
      }
    }
  }
}
