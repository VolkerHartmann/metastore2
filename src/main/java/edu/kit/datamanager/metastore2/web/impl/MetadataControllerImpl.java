/*
 * Copyright 2019 Karlsruhe Institute of Technology.
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
package edu.kit.datamanager.metastore2.web.impl;

import edu.kit.datamanager.entities.RepoUserRole;
import edu.kit.datamanager.exceptions.BadArgumentException;
import edu.kit.datamanager.metastore2.configuration.ApplicationProperties;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.ILinkedMetadataRecordDao;
import edu.kit.datamanager.metastore2.domain.LinkedMetadataRecord;
import edu.kit.datamanager.metastore2.domain.MetadataRecord;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import edu.kit.datamanager.metastore2.util.MetadataRecordUtil;
import edu.kit.datamanager.metastore2.web.IMetadataController;
import edu.kit.datamanager.repo.dao.IDataResourceDao;
import edu.kit.datamanager.repo.dao.spec.dataresource.LastUpdateSpecification;
import edu.kit.datamanager.repo.dao.spec.dataresource.RelatedIdentifierSpec;
import edu.kit.datamanager.repo.dao.spec.dataresource.ResourceTypeSpec;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.repo.domain.ResourceType;
import edu.kit.datamanager.repo.util.DataResourceUtils;
import edu.kit.datamanager.util.ControllerUtils;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

/**
 *
 * @author jejkal
 */
@Controller
@RequestMapping(value = "/api/v1/metadata")
@Schema(description = "Metadata Resource Management")
public class MetadataControllerImpl implements IMetadataController {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataControllerImpl.class);
  @Autowired
  private ApplicationProperties applicationProperties;

  @Autowired
  private ILinkedMetadataRecordDao metadataRecordDao;

  private final MetastoreConfiguration metadataConfig;
  @Autowired
  private final IDataResourceDao dataResourceDao;

  private final String guestToken;

/**
 * 
 * @param applicationProperties
 * @param metadataConfig
 * @param metadataRecordDao
 * @param dataResourceDao 
 */
  public MetadataControllerImpl(ApplicationProperties applicationProperties,
          MetastoreConfiguration metadataConfig,
          ILinkedMetadataRecordDao metadataRecordDao,
          IDataResourceDao dataResourceDao) {
    this.metadataConfig = metadataConfig;
    this.metadataRecordDao = metadataRecordDao;
    this.dataResourceDao = dataResourceDao;
    LOG.info("------------------------------------------------------");
    LOG.info("------{}", this.metadataConfig);
    LOG.info("------------------------------------------------------");
    LOG.trace("Create guest token");
    guestToken = edu.kit.datamanager.util.JwtBuilder.createUserToken("guest", RepoUserRole.GUEST).
            addSimpleClaim("email", "metastore@localhost").
            addSimpleClaim("loginFailures", 0).
            addSimpleClaim("active", true).
            addSimpleClaim("locked", false).getCompactToken(applicationProperties.getJwtSecret());
    MetadataRecordUtil.setToken(guestToken);
  }

  @Override
  public ResponseEntity createRecord(
          @RequestPart(name = "record") final MultipartFile recordDocument,
          @RequestPart(name = "document") final MultipartFile document,
          HttpServletRequest request,
          HttpServletResponse response,
          UriComponentsBuilder uriBuilder) throws URISyntaxException {

     long nano1 = System.nanoTime() / 1000000;
   LOG.trace("Performing createRecord({},...).", recordDocument);
    MetadataRecord record;
    if (recordDocument == null || recordDocument.isEmpty()) {
      String message = "No metadata record provided. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    try {
      record = Json.mapper().readValue(recordDocument.getInputStream(), MetadataRecord.class);
    } catch (IOException ex) {
      String message = "No valid metadata record provided. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    long nano2 = System.nanoTime() / 1000000;

    if (record.getRelatedResource() == null || record.getSchemaId() == null) {
      LOG.error("Mandatory attributes relatedResource and/or schemaId not found in record. Returning HTTP BAD_REQUEST.");
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Mandatory attributes relatedResource and/or schemaId not found in record.");
    }

    if (record.getId() != null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Not expecting record id to be assigned by user.");
    }

    LOG.debug("Test for existing metadata record for given schema and resource");
    boolean recordAlreadyExists = metadataRecordDao.existsMetadataRecordByRelatedResourceAndSchemaId(record.getRelatedResource(), record.getSchemaId());
    long nano3 = System.nanoTime() / 1000000;

    if (recordAlreadyExists) {
      LOG.error("Conflict with existing metadata record!");
      return ResponseEntity.status(HttpStatus.CONFLICT).body("Metadata record already exists! Please update existing record instead!");
    }
    MetadataRecord result = MetadataRecordUtil.createMetadataRecord(metadataConfig, recordDocument, document);
    // Successfully created metadata record.
    long nano4 = System.nanoTime() / 1000000;
    LOG.trace("Metadata record successfully persisted. Returning result.");
    fixMetadataDocumentUri(result);
    long nano5 = System.nanoTime() / 1000000;
    metadataRecordDao.save(new LinkedMetadataRecord(result));
    long nano6 = System.nanoTime() / 1000000;

    URI locationUri;
    locationUri = WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(this.getClass()).getRecordById(result.getId(), result.getRecordVersion(), null, null)).toUri();
    long nano7 = System.nanoTime() / 1000000;
    LOG.error("Create Record Service, {}, {}, {}, {}, {}, {}, {}", nano1, nano2 - nano1, nano3 - nano1, nano4 - nano1, nano5 - nano1, nano6 - nano1, nano7 - nano1);

    return ResponseEntity.created(locationUri).eTag("\"" + result.getEtag() + "\"").body(result);
  }

  @Override
  public ResponseEntity<MetadataRecord> getRecordById(
          @PathVariable(value = "id") String id,
          @RequestParam(value = "version", required = false) Long version,
          WebRequest wr,
          HttpServletResponse hsr
  ) {
    LOG.trace("Performing getRecordById({}, {}).", id, version);

    LOG.trace("Obtaining metadata record with id {} and version {}.", id, version);
    MetadataRecord record = MetadataRecordUtil.getRecordByIdAndVersion(metadataConfig, id, version, true);
    LOG.trace("Metadata record found. Prepare response.");
    //if security enabled, check permission -> if not matching, return HTTP UNAUTHORIZED or FORBIDDEN
    LOG.trace("Get ETag of MetadataRecord.");
    String etag = record.getEtag();
    fixMetadataDocumentUri(record);

    return ResponseEntity.ok().eTag("\"" + etag + "\"").body(record);
  }

  @Override
  public ResponseEntity getMetadataDocumentById(
          @PathVariable(value = "id") String id,
          @RequestParam(value = "version", required = false) Long version,
          WebRequest wr,
          HttpServletResponse hsr
  ) {
    LOG.trace("Performing getMetadataDocumentById({}, {}).", id, version);

    Path metadataDocumentPath = MetadataRecordUtil.getMetadataDocumentByIdAndVersion(metadataConfig, id, version);

    return ResponseEntity.
            ok().
            header(HttpHeaders.CONTENT_LENGTH, String.valueOf(metadataDocumentPath.toFile().length())).
            body(new FileSystemResource(metadataDocumentPath.toFile()));
  }

  public ResponseEntity<List<MetadataRecord>> getAllVersions(
          @PathVariable(value = "id") String id,
          Pageable pgbl
  ) {
    LOG.trace("Performing getAllVersions({}).", id);
    // Search for resource type of MetadataSchemaRecord

    //if security is enabled, include principal in query
    LOG.debug("Performing query for records.");
    Page<DataResource> records = DataResourceUtils.readAllVersionsOfResource(metadataConfig, id, pgbl);
    

    LOG.trace("Transforming Dataresource to MetadataRecord");
    List<DataResource> recordList = records.getContent();
    List<MetadataRecord> metadataList = new ArrayList<>();
    recordList.forEach((record) -> {
      MetadataRecord item = MetadataRecordUtil.migrateToMetadataRecord(metadataConfig, record, false);
      fixMetadataDocumentUri(item);
      metadataList.add(item);
    });

    String contentRange = ControllerUtils.getContentRangeHeader(pgbl.getPageNumber(), pgbl.getPageSize(), records.getTotalElements());

    return ResponseEntity.status(HttpStatus.OK).header("Content-Range", contentRange).body(metadataList);
  }

  @Override
  public ResponseEntity<List<MetadataRecord>> getRecords(
          @RequestParam(value = "id", required = false) String id,
          @RequestParam(value = "resourceId", required = false) List<String> relatedIds,
          @RequestParam(value = "schemaId", required = false) List<String> schemaIds,
          @RequestParam(name = "from", required = false) Instant updateFrom,
          @RequestParam(name = "until", required = false) Instant updateUntil,
          Pageable pgbl,
          WebRequest wr,
          HttpServletResponse hsr,
          UriComponentsBuilder ucb
  ) {
    LOG.trace("Performing getRecords({}, {}, {}, {}).", relatedIds, schemaIds, updateFrom, updateUntil);
    if (id != null) {
      return getAllVersions(id, pgbl);
    } 
    // Search for resource type of MetadataSchemaRecord
    Specification<DataResource> spec = ResourceTypeSpec.toSpecification(ResourceType.createResourceType(MetadataRecord.RESOURCE_TYPE));
    List<String> allRelatedIdentifiers = new ArrayList<>();
    if (schemaIds != null) {
      for (String schemaId : schemaIds) {
        MetadataSchemaRecord currentSchemaRecord = MetadataRecordUtil.getCurrentSchemaRecord(metadataConfig, schemaId);
        for (long versionNumber = 1; versionNumber < currentSchemaRecord.getSchemaVersion(); versionNumber++) {
          allRelatedIdentifiers.add(schemaId + MetadataRecordUtil.SCHEMA_VERSION_SEPARATOR + versionNumber);
        }
      }
    }
    if (relatedIds != null) {
      allRelatedIdentifiers.addAll(relatedIds);
    }
    if (!allRelatedIdentifiers.isEmpty()) {
      spec = spec.and(RelatedIdentifierSpec.toSpecification(allRelatedIdentifiers.toArray(new String[allRelatedIdentifiers.size()])));
    }
    if ((updateFrom != null) || (updateUntil != null)) {
      spec = spec.and(LastUpdateSpecification.toSpecification(updateFrom, updateUntil));
    }

    //if security is enabled, include principal in query
    LOG.debug("Performing query for records.");
    Page<DataResource> records = dataResourceDao.findAll(spec, pgbl);

    LOG.trace("Transforming Dataresource to MetadataRecord");
    List<DataResource> recordList = records.getContent();
    List<MetadataRecord> metadataList = new ArrayList<>();
    recordList.forEach((record) -> {
      MetadataRecord item = MetadataRecordUtil.migrateToMetadataRecord(metadataConfig, record, false);
      fixMetadataDocumentUri(item);
      metadataList.add(item);
    });

    String contentRange = ControllerUtils.getContentRangeHeader(pgbl.getPageNumber(), pgbl.getPageSize(), records.getTotalElements());

    return ResponseEntity.status(HttpStatus.OK).header("Content-Range", contentRange).body(metadataList);
  }

  @Override
  public ResponseEntity updateRecord(
          @PathVariable("id") String id,
          @RequestPart(name = "record", required = false) MultipartFile record,
          @RequestPart(name = "document", required = false)final MultipartFile document,
          WebRequest request,
          HttpServletResponse response,
          UriComponentsBuilder uriBuilder
  ) {
    LOG.trace("Performing updateRecord({}, {}, {}).", id, record, "#document");
    Function<String, String> getById;
    getById = (t) -> {
      return WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(this.getClass()).getRecordById(t, null, request, response)).toString();
    };
    String eTag = ControllerUtils.getEtagFromHeader(request);
    MetadataRecord updateMetadataRecord = MetadataRecordUtil.updateMetadataRecord(metadataConfig, id, eTag, record, document, getById);

    LOG.trace("Metadata record successfully persisted. Updating document URI and returning result.");
    String etag = updateMetadataRecord.getEtag();
    fixMetadataDocumentUri(updateMetadataRecord);

    URI locationUri;
    locationUri = WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(this.getClass()).getRecordById(updateMetadataRecord.getId(), updateMetadataRecord.getRecordVersion(), null, null)).toUri();

    return ResponseEntity.ok().location(locationUri).eTag("\"" + etag + "\"").body(updateMetadataRecord);
  }

  @Override
  public ResponseEntity deleteRecord(
          @PathVariable(value = "id") String id,
          WebRequest wr,
          HttpServletResponse hsr
  ) {
    LOG.trace("Performing deleteRecord({}).", id);
    Function<String, String> getById;
    getById = (t) -> {
      return WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(this.getClass()).getRecordById(t, null, wr, hsr)).toString();
    };
    String eTag = ControllerUtils.getEtagFromHeader(wr);
    MetadataRecordUtil.deleteMetadataRecord(metadataConfig, id, eTag, getById);

    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @Bean
  public RestTemplate restTemplate() {
    return new RestTemplate();
  }

  private void fixMetadataDocumentUri(MetadataRecord record) {
    String metadataDocumentUri = record.getMetadataDocumentUri();
    record.setMetadataDocumentUri(WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(this.getClass()).getMetadataDocumentById(record.getId(), record.getRecordVersion(), null, null)).toUri().toString());
     LOG.trace("Fix metadata document Uri '{}' -> '{}'",metadataDocumentUri, record.getMetadataDocumentUri());
  }
}
