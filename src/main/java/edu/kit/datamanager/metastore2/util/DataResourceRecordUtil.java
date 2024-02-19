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
package edu.kit.datamanager.metastore2.util;

import com.fasterxml.jackson.core.JsonParseException;
import edu.kit.datamanager.clients.SimpleServiceClient;
import edu.kit.datamanager.entities.Identifier;
import edu.kit.datamanager.entities.PERMISSION;
import edu.kit.datamanager.entities.RepoUserRole;
import edu.kit.datamanager.exceptions.AccessForbiddenException;
import edu.kit.datamanager.exceptions.BadArgumentException;
import edu.kit.datamanager.exceptions.CustomInternalServerError;
import edu.kit.datamanager.exceptions.ResourceNotFoundException;
import edu.kit.datamanager.exceptions.UnprocessableEntityException;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.IDataRecordDao;
import edu.kit.datamanager.metastore2.dao.IMetadataFormatDao;
import edu.kit.datamanager.metastore2.dao.ISchemaRecordDao;
import edu.kit.datamanager.metastore2.dao.IUrl2PathDao;
import edu.kit.datamanager.metastore2.domain.DataRecord;
import edu.kit.datamanager.metastore2.domain.MetadataRecord;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import static edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord.SCHEMA_TYPE.JSON;
import static edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord.SCHEMA_TYPE.XML;
import edu.kit.datamanager.metastore2.domain.ResourceIdentifier;
import edu.kit.datamanager.metastore2.domain.ResourceIdentifier.IdentifierType;
import static edu.kit.datamanager.metastore2.domain.ResourceIdentifier.IdentifierType.INTERNAL;
import static edu.kit.datamanager.metastore2.domain.ResourceIdentifier.IdentifierType.URL;
import edu.kit.datamanager.metastore2.domain.SchemaRecord;
import edu.kit.datamanager.metastore2.domain.Url2Path;
import edu.kit.datamanager.metastore2.domain.oaipmh.MetadataFormat;
import static edu.kit.datamanager.metastore2.util.MetadataSchemaRecordUtil.fixRelativeURI;
import edu.kit.datamanager.metastore2.validation.IValidator;
import edu.kit.datamanager.metastore2.web.impl.MetadataControllerImpl;
import edu.kit.datamanager.repo.configuration.RepoBaseConfiguration;
import edu.kit.datamanager.repo.domain.ContentInformation;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.repo.domain.Date;
import edu.kit.datamanager.repo.domain.RelatedIdentifier;
import edu.kit.datamanager.repo.domain.ResourceType;
import edu.kit.datamanager.repo.domain.Scheme;
import edu.kit.datamanager.repo.domain.Title;
import edu.kit.datamanager.repo.domain.acl.AclEntry;
import edu.kit.datamanager.repo.service.IContentInformationService;
import edu.kit.datamanager.repo.util.ContentDataUtils;
import edu.kit.datamanager.repo.util.DataResourceUtils;
import edu.kit.datamanager.util.AuthenticationHelper;
import edu.kit.datamanager.util.ControllerUtils;
import io.swagger.v3.core.util.Json;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Utility class for handling json documents
 */
public class DataResourceRecordUtil {

  /**
   * Separator for separating schemaId and schemaVersion.
   */
  public static final String SCHEMA_VERSION_SEPARATOR = ":";
  /**
   * Logger for messages.
   */
  private static final Logger LOG = LoggerFactory.getLogger(DataResourceRecordUtil.class);

  private static final String LOG_ERROR_READ_METADATA_DOCUMENT = "Failed to read metadata document from input stream.";
  private static final String LOG_SEPARATOR = "-----------------------------------------";
  private static final String LOG_SCHEMA_REGISTRY = "No external schema registries defined. Try to use internal one...";
  private static final String LOG_FETCH_SCHEMA = "Try to fetch schema from '{}'.";
  private static final String PATH_SCHEMA = "schemas";
  private static final String LOG_ERROR_ACCESS = "Failed to access schema registry at '%s'. Proceeding with next registry.";
  private static final String LOG_SCHEMA_RECORD = "Found schema record: '{}'";
  private static final String ERROR_PARSING_JSON = "Error parsing json: ";

  private static MetastoreConfiguration schemaConfig;
  private static String guestToken = null;

  private static IDataRecordDao dataRecordDao;
  private static ISchemaRecordDao schemaRecordDao;
  private static IMetadataFormatDao metadataFormatDao;

  private static IUrl2PathDao url2PathDao;

  DataResourceRecordUtil() {
    //Utility class
  }

  /**
   * Create/Ingest an instance of MetadataSchemaRecord.
   *
   * @param applicationProperties Settings of repository.
   * @param recordDocument Record of the schema.
   * @param document Schema document.
   * @param getSchemaDocumentById Method for creating access URL.
   * @return Record of registered schema document.
   */
  public static DataResource createDataResourceRecord4Schema(MetastoreConfiguration applicationProperties,
          MultipartFile recordDocument,
          MultipartFile document,
          BiFunction<String, Long, String> getSchemaDocumentById) {
    DataResource metadataRecord;

    // Do some checks first.
    metadataRecord = checkParameters(recordDocument, document, true);
    
    if (metadataRecord.getId() == null) {
      String message = "Mandatory attribute 'id' not found in record. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    // Check if id is lower case and URL encodable. 
    DataResourceRecordUtil.check4validId(metadataRecord);
    // Create schema record
    SchemaRecord schemaRecord = new SchemaRecord();
    schemaRecord.setSchemaId(metadataRecord.getId());
    if (!metadataRecord.getFormats().isEmpty()) {
      String mimeType = metadataRecord.getFormats().iterator().next().toLowerCase();
      if (mimeType.contains("json")) {
        schemaRecord.setType(JSON);
      } else if (mimeType.contains("xml")) {
        schemaRecord.setType(XML);
      }
    }
    // End of parameter checks
    // validate schema document / determine type if not given
    validateMetadataSchemaDocument(applicationProperties, schemaRecord, document);
    // set internal parameters
    if (metadataRecord.getFormats().isEmpty()) {
      LOG.trace("No mimetype set! Try to determine...");
      if (document.getContentType() != null) {
        LOG.trace("Set mimetype determined from document: '{}'", document.getContentType());
        metadataRecord.getFormats().add(document.getContentType());
      } else {
        LOG.trace("Set mimetype according to type '{}'.", schemaRecord.getType());
        switch (schemaRecord.getType()) {
          case JSON:
            metadataRecord.getFormats().add(MediaType.APPLICATION_JSON_VALUE);
            break;
          case XML:
            metadataRecord.getFormats().add(MediaType.APPLICATION_XML_VALUE);
            break;
          default:
            throw new BadArgumentException("Please provide mimetype for type '" + schemaRecord.getType() + "'");
        }
      }
    }
    metadataRecord.setVersion(Long.toString(1));
    // create record.
    DataResource dataResource = metadataRecord;
    DataResource createResource = DataResourceUtils.createResource(applicationProperties, dataResource);
    // store document
    ContentInformation contentInformation = ContentDataUtils.addFile(applicationProperties, createResource, document, document.getOriginalFilename(), null, true, t -> "somethingStupid");
    schemaRecord.setVersion(applicationProperties.getAuditService().getCurrentVersion(dataResource.getId()));
    schemaRecord.setSchemaDocumentUri(contentInformation.getContentUri());
    schemaRecord.setDocumentHash(contentInformation.getHash());
    saveNewSchemaRecord(schemaRecord);
    // Settings for OAI PMH
    if (MetadataSchemaRecord.SCHEMA_TYPE.XML.equals(schemaRecord.getType())) {
      try {
        MetadataFormat metadataFormat = new MetadataFormat();
        metadataFormat.setMetadataPrefix(schemaRecord.getSchemaId());
        metadataFormat.setSchema(getSchemaDocumentById.apply(schemaRecord.getSchemaId(), schemaRecord.getVersion()));
        String metadataNamespace = SchemaUtils.getTargetNamespaceFromSchema(document.getBytes());
        metadataFormat.setMetadataNamespace(metadataNamespace);
        metadataFormatDao.save(metadataFormat);
      } catch (IOException ex) {
        String message = LOG_ERROR_READ_METADATA_DOCUMENT;
        LOG.error(message, ex);
        throw new UnprocessableEntityException(message);
      }
    }

    return metadataRecord;
  }

  /**
   * Create a digital object from metadata record and metadata document.
   *
   * @param applicationProperties Configuration properties.
   * @param recordDocument Metadata record.
   * @param document Metadata document.
   * @return Enriched metadata record.
   */
  public static MetadataRecord createDataResource4MetadataDocument(MetastoreConfiguration applicationProperties,
          MultipartFile recordDocument, MultipartFile document) {
    DataResource metadataRecord;
    long nano1 = System.nanoTime() / 1000000;
    // Do some checks first.
    metadataRecord = checkParameters(recordDocument, document, true);

    if (metadataRecord.getRelatedResource() == null || metadataRecord.getRelatedResource().getIdentifier() == null || metadataRecord.getSchema() == null || metadataRecord.getSchema().getIdentifier() == null) {
      String message = "Mandatory attributes relatedResource and/or schema not found in record. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    // Test for schema version
    if (metadataRecord.getSchemaVersion() == null) {
      MetadataSchemaRecord currentSchemaRecord;
      try {
        currentSchemaRecord = MetadataSchemaRecordUtil.getCurrentSchemaRecord(applicationProperties, metadataRecord.getSchema());
      } catch (ResourceNotFoundException rnfe) {
        throw new UnprocessableEntityException("Unknown schema ID '" + metadataRecord.getSchema().getIdentifier() + "'!");
      }
      metadataRecord.setSchemaVersion(currentSchemaRecord.getSchemaVersion());
    }

    // validate document
    long nano2 = System.nanoTime() / 1000000;
    // validate schema document
    validateMetadataDocument(applicationProperties, metadataRecord, document);
    // set internal parameters
    metadataRecord.setRecordVersion(1l);

    long nano3 = System.nanoTime() / 1000000;
    // create record.
    DataResource dataResource = migrateToDataResource(applicationProperties, metadataRecord);
    // add id as internal identifier if exists
    // Note: DataResourceUtils.createResource will ignore id of resource. 
    // id will be set to alternate identifier if exists. 
    if (dataResource.getId() != null) {
      // check for valid identifier without any chars which may be encoded
      try {
        String originalId = dataResource.getId();
        String value = URLEncoder.encode(originalId, StandardCharsets.UTF_8.toString());
        if (!value.equals(originalId)) {
          String message = "Not a valid id! Encoded: " + value;
          LOG.error(message);
          throw new BadArgumentException(message);
        }
      } catch (UnsupportedEncodingException ex) {
        String message = "Error encoding id " + metadataRecord.getSchemaId();
        LOG.error(message);
        throw new CustomInternalServerError(message);
      }

      dataResource.getAlternateIdentifiers().add(Identifier.factoryInternalIdentifier(dataResource.getId()));
    }
    long nano4 = System.nanoTime() / 1000000;
    DataResource createResource = DataResourceUtils.createResource(applicationProperties, dataResource);
    long nano5 = System.nanoTime() / 1000000;
    // store document
    ContentInformation contentInformation = ContentDataUtils.addFile(applicationProperties, createResource, document, document.getOriginalFilename(), null, true, t -> "somethingStupid");
    long nano6 = System.nanoTime() / 1000000;
    // Create additional metadata record for faster access
    DataRecord dataRecord = new DataRecord();
    dataRecord.setMetadataId(createResource.getId());
    dataRecord.setVersion(metadataRecord.getRecordVersion());
    dataRecord.setSchemaId(metadataRecord.getSchema().getIdentifier());
    dataRecord.setSchemaVersion(metadataRecord.getSchemaVersion());
    dataRecord.setMetadataDocumentUri(contentInformation.getContentUri());
    dataRecord.setDocumentHash(contentInformation.getHash());
    dataRecord.setLastUpdate(dataResource.getLastUpdate());
    saveNewDataRecord(dataRecord);
    long nano7 = System.nanoTime() / 1000000;
    LOG.info("Create Record times, {}, {}, {}, {}, {}, {}, {}", nano1, nano2 - nano1, nano3 - nano1, nano4 - nano1, nano5 - nano1, nano6 - nano1, nano7 - nano1);

    return migrateToMetadataRecord(applicationProperties, createResource, true);
  }

  /**
   * Update a digital object with given metadata record and/or metadata
   * document.
   *
   * @param applicationProperties Configuration properties.
   * @param resourceId Identifier of digital object.
   * @param eTag ETag of the old digital object.
   * @param recordDocument Metadata record.
   * @param document Metadata document.
   * @param supplier Function for updating record.
   * @return Enriched metadata record.
   */
  public static MetadataRecord updateMetadataRecord(MetastoreConfiguration applicationProperties,
          String resourceId,
          String eTag,
          MultipartFile recordDocument,
          MultipartFile document,
          UnaryOperator<String> supplier) {
    MetadataRecord metadataRecord = null;
    MetadataRecord existingRecord;

    // Do some checks first.
    if ((recordDocument == null || recordDocument.isEmpty()) && (document == null || document.isEmpty())) {
      String message = "Neither metadata record nor metadata document provided.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    if (!(recordDocument == null || recordDocument.isEmpty())) {
      try {
        metadataRecord = Json.mapper().readValue(recordDocument.getInputStream(), MetadataRecord.class);
      } catch (IOException ex) {
        String message = "Can't map record document to MetadataRecord";
        if (ex instanceof JsonParseException) {
          message = message + " Reason: " + ex.getMessage();
        }
        LOG.error(ERROR_PARSING_JSON, ex);
        throw new BadArgumentException(message);
      }
    }

    LOG.trace("Obtaining most recent metadata record with id {}.", resourceId);
    DataResource dataResource = applicationProperties.getDataResourceService().findById(resourceId);
    LOG.trace("Checking provided ETag.");
    ControllerUtils.checkEtag(eTag, dataResource);
    if (metadataRecord != null) {
      existingRecord = migrateToMetadataRecord(applicationProperties, dataResource, false);
      existingRecord = mergeRecords(existingRecord, metadataRecord);
      dataResource = migrateToDataResource(applicationProperties, existingRecord);
    } else {
      dataResource = DataResourceUtils.copyDataResource(dataResource);
    }

    boolean noChanges = false;
    if (document != null) {
      metadataRecord = migrateToMetadataRecord(applicationProperties, dataResource, false);
      validateMetadataDocument(applicationProperties, metadataRecord, document);

      ContentInformation info;
      String fileName = document.getOriginalFilename();
      info = getContentInformationOfResource(applicationProperties, dataResource);
      if (info != null) {
        fileName = info.getRelativePath();
        noChanges = true;
        // Check for changes...
        try {
          byte[] currentFileContent;
          File file = new File(URI.create(info.getContentUri()));
          if (document.getSize() == Files.size(file.toPath())) {
            currentFileContent = FileUtils.readFileToByteArray(file);
            byte[] newFileContent = document.getBytes();
            for (int index = 0; index < currentFileContent.length; index++) {
              if (currentFileContent[index] != newFileContent[index]) {
                noChanges = false;
                break;
              }
            }
          } else {
            noChanges = false;
          }
        } catch (IOException ex) {
          LOG.error("Error reading current file!", ex);
        }
      }
      if (!noChanges) {
        // Everything seems to be fine update document and increment version
        LOG.trace("Updating schema document (and increment version)...");
        String version = dataResource.getVersion();
        if (version != null) {
          dataResource.setVersion(Long.toString(Long.parseLong(version) + 1l));
        }
        ContentDataUtils.addFile(applicationProperties, dataResource, document, fileName, null, true, supplier);
      }

    } else {
      // validate if document is still valid due to changed record settings.
      metadataRecord = migrateToMetadataRecord(applicationProperties, dataResource, false);
      URI metadataDocumentUri = URI.create(metadataRecord.getMetadataDocumentUri());

      Path metadataDocumentPath = Paths.get(metadataDocumentUri);
      if (!Files.exists(metadataDocumentPath) || !Files.isRegularFile(metadataDocumentPath) || !Files.isReadable(metadataDocumentPath)) {
        LOG.warn("Metadata document at path {} either does not exist or is no file or is not readable. Returning HTTP NOT_FOUND.", metadataDocumentPath);
        throw new CustomInternalServerError("Metadata document on server either does not exist or is no file or is not readable.");
      }

      try {
        InputStream inputStream = Files.newInputStream(metadataDocumentPath);
        SchemaRecord schemaRecord = MetadataSchemaRecordUtil.getSchemaRecord(metadataRecord.getSchema(), metadataRecord.getSchemaVersion());
        MetadataSchemaRecordUtil.validateMetadataDocument(applicationProperties, inputStream, schemaRecord);
      } catch (IOException ex) {
        LOG.error("Error validating file!", ex);
      }

    }
    if (noChanges) {
      Optional<DataRecord> dataRecord = dataRecordDao.findTopByMetadataIdOrderByVersionDesc(dataResource.getId());
      if (dataRecord.isPresent()) {
        dataRecordDao.delete(dataRecord.get());
      }
    }
    dataResource = DataResourceUtils.updateResource(applicationProperties, resourceId, dataResource, eTag, supplier);

    return migrateToMetadataRecord(applicationProperties, dataResource, true);
  }

  /**
   * Delete a digital object with given identifier.
   *
   * @param applicationProperties Configuration properties.
   * @param id Identifier of digital object.
   * @param eTag ETag of the old digital object.
   * @param supplier Function for updating record.
   */
  public static void deleteMetadataRecord(MetastoreConfiguration applicationProperties,
          String id,
          String eTag,
          UnaryOperator<String> supplier) {
    DataResourceUtils.deleteResource(applicationProperties, id, eTag, supplier);
    try {
      DataResourceUtils.getResourceByIdentifierOrRedirect(applicationProperties, id, null, supplier);
    } catch (ResourceNotFoundException rnfe) {
      Optional<DataRecord> dataRecord = dataRecordDao.findTopByMetadataIdOrderByVersionDesc(id);
      while (dataRecord.isPresent()) {
        dataRecordDao.delete(dataRecord.get());
        dataRecord = dataRecordDao.findTopByMetadataIdOrderByVersionDesc(id);
      }
    }
  }

  /**
   * Migrate metadata record to data resource.
   *
   * @param applicationProperties Configuration settings of repository.
   * @param metadataRecord Metadata record to migrate.
   * @return Data resource of metadata record.
   */
  public static DataResource migrateToDataResource(RepoBaseConfiguration applicationProperties,
          MetadataRecord metadataRecord) {
    DataResource dataResource;
    if (metadataRecord.getId() != null) {
      try {
        dataResource = applicationProperties.getDataResourceService().findById(metadataRecord.getId(), metadataRecord.getRecordVersion());
        dataResource = DataResourceUtils.copyDataResource(dataResource);
      } catch (ResourceNotFoundException rnfe) {
        LOG.error("Error catching DataResource for " + metadataRecord.getId() + " -> " + rnfe.getMessage());
        dataResource = DataResource.factoryNewDataResource(metadataRecord.getId());
        dataResource.setVersion("1");
      }
    } else {
      dataResource = new DataResource();
      dataResource.setVersion("1");
    }
    dataResource.setAcls(metadataRecord.getAcl());
    if (metadataRecord.getCreatedAt() != null) {
      boolean createDateExists = false;
      Set<Date> dates = dataResource.getDates();
      for (edu.kit.datamanager.repo.domain.Date d : dates) {
        if (edu.kit.datamanager.repo.domain.Date.DATE_TYPE.CREATED.equals(d.getType())) {
          LOG.trace("Creation date entry found.");
          createDateExists = true;
          break;
        }
      }
      if (!createDateExists) {
        dataResource.getDates().add(Date.factoryDate(metadataRecord.getCreatedAt(), Date.DATE_TYPE.CREATED));
      }
    }
    Set<Identifier> identifiers = dataResource.getAlternateIdentifiers();
    if (metadataRecord.getPid() != null) {
      ResourceIdentifier identifier = metadataRecord.getPid();
      MetadataSchemaRecordUtil.checkAlternateIdentifier(identifiers, identifier.getIdentifier(), Identifier.IDENTIFIER_TYPE.valueOf(identifier.getIdentifierType().name()));
    } else {
      LOG.trace("Remove existing identifiers (others than URL)...");
      Set<Identifier> removeItems = new HashSet<>();
      for (Identifier item : identifiers) {
        if (item.getIdentifierType() != Identifier.IDENTIFIER_TYPE.URL) {
          LOG.trace("... {},  {}", item.getValue(), item.getIdentifierType());
          removeItems.add(item);
        }
      }
      identifiers.removeAll(removeItems);
    }
    boolean relationFound = false;
    boolean schemaIdFound = false;
    for (RelatedIdentifier relatedIds : dataResource.getRelatedIdentifiers()) {
      if (relatedIds.getRelationType() == RelatedIdentifier.RELATION_TYPES.IS_METADATA_FOR) {
        LOG.trace("Set relation to '{}'", metadataRecord.getRelatedResource());
        relatedIds.setValue(metadataRecord.getRelatedResource().getIdentifier());
        relatedIds.setIdentifierType(Identifier.IDENTIFIER_TYPE.valueOf(metadataRecord.getRelatedResource().getIdentifierType().name()));
        relationFound = true;
      }
      if (relatedIds.getRelationType() == RelatedIdentifier.RELATION_TYPES.IS_DERIVED_FROM) {
        updateRelatedIdentifierForSchema(relatedIds, metadataRecord);
        schemaIdFound = true;
      }
    }
    if (!relationFound) {
      RelatedIdentifier relatedResource = RelatedIdentifier.factoryRelatedIdentifier(RelatedIdentifier.RELATION_TYPES.IS_METADATA_FOR, metadataRecord.getRelatedResource().getIdentifier(), null, null);
      relatedResource.setIdentifierType(Identifier.IDENTIFIER_TYPE.valueOf(metadataRecord.getRelatedResource().getIdentifierType().name()));
      dataResource.getRelatedIdentifiers().add(relatedResource);
    }
    if (!schemaIdFound) {
      RelatedIdentifier schemaId = updateRelatedIdentifierForSchema(null, metadataRecord);
      dataResource.getRelatedIdentifiers().add(schemaId);
    }
    String defaultTitle = "Metadata 4 metastore";
    boolean titleExists = false;
    for (Title title : dataResource.getTitles()) {
      if (title.getTitleType() == Title.TYPE.OTHER && title.getValue().equals(defaultTitle)) {
        titleExists = true;
      }
    }
    if (!titleExists) {
      dataResource.getTitles().add(Title.factoryTitle(defaultTitle, Title.TYPE.OTHER));
    }
    dataResource.setResourceType(ResourceType.createResourceType(MetadataRecord.RESOURCE_TYPE));
    checkLicense(dataResource, metadataRecord.getLicenseUri());

    return dataResource;
  }

  /**
   * Migrate data resource to metadata record.
   *
   * @param applicationProperties Configuration settings of repository.
   * @param dataResource Data resource to migrate.
   * @param provideETag Flag for calculating etag.
   * @return Metadata record of data resource.
   */
  public static MetadataRecord migrateToMetadataRecord(RepoBaseConfiguration applicationProperties,
          DataResource dataResource,
          boolean provideETag) {
    long nano1 = System.nanoTime() / 1000000;
    MetadataRecord metadataRecord = new MetadataRecord();
    if (dataResource != null) {
      metadataRecord.setId(dataResource.getId());
      if (provideETag) {
        metadataRecord.setETag(dataResource.getEtag());
      }
      metadataRecord.setAcl(dataResource.getAcls());

      for (edu.kit.datamanager.repo.domain.Date d : dataResource.getDates()) {
        if (edu.kit.datamanager.repo.domain.Date.DATE_TYPE.CREATED.equals(d.getType())) {
          LOG.trace("Creation date entry found.");
          metadataRecord.setCreatedAt(d.getValue());
          break;
        }
      }
      if (dataResource.getLastUpdate() != null) {
        metadataRecord.setLastUpdate(dataResource.getLastUpdate());
      }

      for (Identifier identifier : dataResource.getAlternateIdentifiers()) {
        if (identifier.getIdentifierType() != Identifier.IDENTIFIER_TYPE.URL) {
          if (identifier.getIdentifierType() != Identifier.IDENTIFIER_TYPE.INTERNAL) {
            ResourceIdentifier resourceIdentifier = ResourceIdentifier.factoryResourceIdentifier(identifier.getValue(), ResourceIdentifier.IdentifierType.valueOf(identifier.getIdentifierType().getValue()));
            LOG.trace("Set PID to '{}' of type '{}'", resourceIdentifier.getIdentifier(), resourceIdentifier.getIdentifierType());
            metadataRecord.setPid(resourceIdentifier);
            break;
          } else {
            LOG.debug("'INTERNAL' identifier shouldn't be used! Migrate them to 'URL' if possible.");
          }
        }
      }

      Long recordVersion = 1l;
      if (dataResource.getVersion() != null) {
        recordVersion = Long.parseLong(dataResource.getVersion());
      }
      metadataRecord.setRecordVersion(recordVersion);

      for (RelatedIdentifier relatedIds : dataResource.getRelatedIdentifiers()) {
        LOG.trace("Found related Identifier: '{}'", relatedIds);
        if (relatedIds.getRelationType() == RelatedIdentifier.RELATION_TYPES.IS_METADATA_FOR) {
          ResourceIdentifier resourceIdentifier = ResourceIdentifier.factoryInternalResourceIdentifier(relatedIds.getValue());
          if (relatedIds.getIdentifierType() != null) {
            resourceIdentifier = ResourceIdentifier.factoryResourceIdentifier(relatedIds.getValue(), IdentifierType.valueOf(relatedIds.getIdentifierType().name()));
          }
          LOG.trace("Set relation to '{}'", resourceIdentifier);
          metadataRecord.setRelatedResource(resourceIdentifier);
        }
        if (relatedIds.getRelationType() == RelatedIdentifier.RELATION_TYPES.IS_DERIVED_FROM) {
          ResourceIdentifier resourceIdentifier = ResourceIdentifier.factoryResourceIdentifier(relatedIds.getValue(), IdentifierType.valueOf(relatedIds.getIdentifierType().name()));
          metadataRecord.setSchema(resourceIdentifier);
          if (resourceIdentifier.getIdentifierType().equals(IdentifierType.URL)) {
            //Try to fetch version from URL (only works with URLs including the version as query parameter.
            Matcher matcher = Pattern.compile(".*[&?]version=(\\d*).*").matcher(resourceIdentifier.getIdentifier());
            while (matcher.find()) {
              metadataRecord.setSchemaVersion(Long.parseLong(matcher.group(1)));
            }
          } else {
            metadataRecord.setSchemaVersion(1l);
          }
          LOG.trace("Set schema to '{}'", resourceIdentifier);
        }
      }
      if (metadataRecord.getSchema() == null) {
        String message = "Missing schema identifier for metadata document. Not a valid metadata document ID. Returning HTTP BAD_REQUEST.";
        LOG.error(message);
        throw new BadArgumentException(message);
      }
      DataRecord dataRecord = null;
      long nano2 = System.nanoTime() / 1000000;
      Optional<DataRecord> dataRecordResult = dataRecordDao.findByMetadataIdAndVersion(dataResource.getId(), recordVersion);
      long nano3 = System.nanoTime() / 1000000;
      long nano4 = nano3;
      boolean isAvailable = false;
      boolean saveDataRecord = false;
      if (dataRecordResult.isPresent()) {
        LOG.trace("Get document URI from DataRecord.");
        dataRecord = dataRecordResult.get();
        nano4 = System.nanoTime() / 1000000;
        metadataRecord.setMetadataDocumentUri(dataRecord.getMetadataDocumentUri());
        metadataRecord.setDocumentHash(dataRecord.getDocumentHash());
        metadataRecord.setSchemaVersion(dataRecord.getSchemaVersion());
        isAvailable = true;
      } else {
        saveDataRecord = true;
      }
      if (!isAvailable) {
        LOG.trace("Get document URI from ContentInformation.");
        ContentInformation info;
        info = getContentInformationOfResource(applicationProperties, dataResource);
        nano4 = System.nanoTime() / 1000000;
        if (info != null) {
          metadataRecord.setDocumentHash(info.getHash());
          metadataRecord.setMetadataDocumentUri(info.getContentUri());
          MetadataSchemaRecord currentSchemaRecord = MetadataSchemaRecordUtil.getCurrentSchemaRecord(schemaConfig, metadataRecord.getSchema());
          metadataRecord.setSchemaVersion(currentSchemaRecord.getSchemaVersion());
          if (saveDataRecord) {
            saveNewDataRecord(metadataRecord);
          }
        }
      }
      // Only one license allowed. So don't worry about size of set.
      if (!dataResource.getRights().isEmpty()) {
        metadataRecord.setLicenseUri(dataResource.getRights().iterator().next().getSchemeUri());
      }
      long nano5 = System.nanoTime() / 1000000;
      LOG.info("Migrate to MetadataRecord, {}, {}, {}, {}, {}, {}", nano1, nano2 - nano1, nano3 - nano1, nano4 - nano1, nano5 - nano1, provideETag);
    }

    return metadataRecord;
  }

  private static ContentInformation getContentInformationOfResource(RepoBaseConfiguration applicationProperties,
          DataResource dataResource) {
    ContentInformation returnValue = null;
    long nano1 = System.nanoTime() / 1000000;
    IContentInformationService contentInformationService = applicationProperties.getContentInformationService();
    ContentInformation info = new ContentInformation();
    info.setParentResource(dataResource);
    long nano2 = System.nanoTime() / 1000000;
    List<ContentInformation> listOfFiles = contentInformationService.findAll(info, PageRequest.of(0, 100)).getContent();
    long nano3 = System.nanoTime() / 1000000;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Found {} files for resource '{}'", listOfFiles.size(), dataResource.getId());
      for (ContentInformation ci : listOfFiles) {
        DataResource parentResource = ci.getParentResource();
        ci.setParentResource(null);
        LOG.trace("ContentInformation: {}", ci);
        ci.setParentResource(parentResource);
      }
    }
    if (!listOfFiles.isEmpty()) {
      returnValue = listOfFiles.get(0);
    }
    LOG.info("Get content information of resource, {}, {}, {}, {}, {}, {}", nano1, nano2 - nano1, nano3 - nano1);
    return returnValue;
  }

  /**
   * Returns schema record with the current version.
   *
   * @param metastoreProperties Configuration for accessing services
   * @param schemaId SchemaID of the schema.
   * @return MetadataSchemaRecord ResponseEntity in case of an error.
   */
  public static MetadataSchemaRecord getCurrentInternalSchemaRecord(MetastoreConfiguration metastoreProperties,
          String schemaId) {
    LOG.trace("Get current internal schema record for id '{}'.", schemaId);
    MetadataSchemaRecord returnValue = null;
    boolean success = false;
    StringBuilder errorMessage = new StringBuilder();
    if (metastoreProperties.getSchemaRegistries().size() == 0) {
      LOG.trace(LOG_SCHEMA_REGISTRY);

      returnValue = MetadataSchemaRecordUtil.getRecordById(metastoreProperties, schemaId);
      success = true;
    } else {
      for (String schemaRegistry : metastoreProperties.getSchemaRegistries()) {
        LOG.trace(LOG_FETCH_SCHEMA, schemaRegistry);
        URI schemaRegistryUri = URI.create(schemaRegistry);
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance().scheme(schemaRegistryUri.getScheme()).host(schemaRegistryUri.getHost()).port(schemaRegistryUri.getPort()).pathSegment(schemaRegistryUri.getPath(), PATH_SCHEMA, schemaId);

        URI finalUri = builder.build().toUri();

        try {
          returnValue = SimpleServiceClient.create(finalUri.toString()).withBearerToken(guestToken).accept(MetadataSchemaRecord.METADATA_SCHEMA_RECORD_MEDIA_TYPE).getResource(MetadataSchemaRecord.class
          );
          success = true;
          break;
        } catch (HttpClientErrorException ce) {
          String message = "Error accessing schema '" + schemaId + "' at '" + schemaRegistry + "'!";
          LOG.error(message, ce);
          errorMessage.append(message).append("\n");
        } catch (RestClientException ex) {
          String message = String.format(LOG_ERROR_ACCESS, schemaRegistry);
          LOG.error(message, ex);
          errorMessage.append(message).append("\n");
        }
      }
    }
    if (!success) {
      throw new UnprocessableEntityException(errorMessage.toString());
    }
    LOG.trace(LOG_SCHEMA_RECORD, returnValue);
    return returnValue;
  }

  /**
   * Returns schema record with the current version.
   *
   * @param metastoreProperties Configuration for accessing services
   * @param schemaId SchemaID of the schema.
   * @param version Version of the schema.
   * @return MetadataSchemaRecord ResponseEntity in case of an error.
   */
  public static MetadataSchemaRecord getInternalSchemaRecord(MetastoreConfiguration metastoreProperties,
          String schemaId,
          Long version) {
    MetadataSchemaRecord returnValue = null;
    boolean success = false;
    StringBuilder errorMessage = new StringBuilder();
    LOG.trace("Get internal schema record for id '{}'.", schemaId);
    if (metastoreProperties.getSchemaRegistries().size() == 0) {
      LOG.trace(LOG_SCHEMA_REGISTRY);

      returnValue = MetadataSchemaRecordUtil.getRecordByIdAndVersion(metastoreProperties, schemaId, version);
      success = true;
    } else {
      for (String schemaRegistry : metastoreProperties.getSchemaRegistries()) {
        LOG.trace(LOG_FETCH_SCHEMA, schemaRegistry);
        URI schemaRegistryUri = URI.create(schemaRegistry);
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance().scheme(schemaRegistryUri.getScheme()).host(schemaRegistryUri.getHost()).port(schemaRegistryUri.getPort()).pathSegment(schemaRegistryUri.getPath(), PATH_SCHEMA, schemaId).queryParam("version", version);

        URI finalUri = builder.build().toUri();

        try {
          returnValue = SimpleServiceClient.create(finalUri.toString()).withBearerToken(guestToken).accept(MetadataSchemaRecord.METADATA_SCHEMA_RECORD_MEDIA_TYPE).getResource(MetadataSchemaRecord.class
          );
          success = true;
          break;
        } catch (HttpClientErrorException ce) {
          String message = "Error accessing schema '" + schemaId + "' at '" + schemaRegistry + "'!";
          LOG.error(message, ce);
          errorMessage.append(message).append("\n");
        } catch (RestClientException ex) {
          String message = String.format(LOG_ERROR_ACCESS, schemaRegistry);
          LOG.error(message, ex);
          errorMessage.append(message).append("\n");
        }
      }
    }
    if (!success) {
      throw new UnprocessableEntityException(errorMessage.toString());
    }
    LOG.trace(LOG_SCHEMA_RECORD, returnValue);
    return returnValue;
  }

  /**
   * Update/create related identifier to values given by metadata record.
   *
   * @param relatedIdentifier related identifier (if null create a new one)
   * @param metadataRecord record holding schema information.
   * @return updated/created related identifier.
   */
  private static RelatedIdentifier updateRelatedIdentifierForSchema(RelatedIdentifier relatedIdentifier, MetadataRecord metadataRecord) {
    if (relatedIdentifier == null) {
      relatedIdentifier = RelatedIdentifier.factoryRelatedIdentifier(RelatedIdentifier.RELATION_TYPES.IS_DERIVED_FROM, null, null, null);
    }
    ResourceIdentifier schemaIdentifier = MetadataSchemaRecordUtil.getSchemaIdentifier(schemaConfig, metadataRecord);
    relatedIdentifier.setIdentifierType(Identifier.IDENTIFIER_TYPE.valueOf(schemaIdentifier.getIdentifierType().name()));
    relatedIdentifier.setValue(schemaIdentifier.getIdentifier());
    LOG.trace("Set relatedId for schema to '{}'", relatedIdentifier);

    return relatedIdentifier;
  }

  /**
   * Validate metadata document with given schema.
   *
   * @param metastoreProperties Configuration for accessing services
   * @param metadataRecord metadata of the document.
   * @param document document
   */
  private static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
          MetadataRecord metadataRecord,
          MultipartFile document) {
    LOG.trace("validateMetadataDocument {},{}, {}", metastoreProperties, metadataRecord, document);
    if (document == null || document.isEmpty()) {
      String message = "Missing metadata document in body. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    boolean validationSuccess = false;
    StringBuilder errorMessage = new StringBuilder();
    if (metastoreProperties.getSchemaRegistries().isEmpty() || metadataRecord.getSchema().getIdentifierType() != IdentifierType.INTERNAL) {
      LOG.trace(LOG_SCHEMA_REGISTRY);
      if (schemaConfig != null) {
        try {
          MetadataSchemaRecordUtil.validateMetadataDocument(schemaConfig, document, metadataRecord.getSchema(), metadataRecord.getSchemaVersion());
          validationSuccess = true;
        } catch (Exception ex) {
          String message = "Error validating document!";
          LOG.error(message, ex);
          errorMessage.append(ex.getMessage()).append("\n");
        }
      } else {
        throw new CustomInternalServerError("No schema registries defined! ");
      }
    } else {
      for (String schemaRegistry : metastoreProperties.getSchemaRegistries()) {
        LOG.trace(LOG_FETCH_SCHEMA, schemaRegistry);
        URI schemaRegistryUri = URI.create(schemaRegistry);
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance().scheme(schemaRegistryUri.getScheme()).host(schemaRegistryUri.getHost()).port(schemaRegistryUri.getPort()).pathSegment(schemaRegistryUri.getPath(), PATH_SCHEMA, metadataRecord.getSchema().getIdentifier(), "validate").queryParam("version", metadataRecord.getSchemaVersion());

        URI finalUri = builder.build().toUri();

        try {
          HttpStatus status = SimpleServiceClient.create(finalUri.toString()).withBearerToken(guestToken).accept(MetadataSchemaRecord.METADATA_SCHEMA_RECORD_MEDIA_TYPE).withFormParam("document", document.getInputStream()).postForm(MediaType.MULTIPART_FORM_DATA);

          if (Objects.equals(HttpStatus.NO_CONTENT, status)) {
            LOG.trace("Successfully validated document against schema {} in registry {}.", metadataRecord.getSchema().getIdentifier(), schemaRegistry);
            validationSuccess = true;
            break;
          }
        } catch (HttpClientErrorException ce) {
          //not valid 
          String message = "Failed to validate metadata document against schema " + metadataRecord.getSchema().getIdentifier() + " at '" + schemaRegistry + "' with status " + ce.getStatusCode() + ".";
          LOG.error(message, ce);
          errorMessage.append(message).append("\n");
        } catch (IOException | RestClientException ex) {
          String message = String.format(LOG_ERROR_ACCESS, schemaRegistry);
          LOG.error(message, ex);
          errorMessage.append(message).append("\n");
        }
      }
    }
    if (!validationSuccess) {
      throw new UnprocessableEntityException(errorMessage.toString());
    }
  }

  public static DataResource getRecordById(MetastoreConfiguration metastoreProperties,
          String recordId) throws ResourceNotFoundException {
    return getRecordByIdAndVersion(metastoreProperties, recordId, null);
  }


  public static DataResource getRecordByIdAndVersion(MetastoreConfiguration metastoreProperties,
          String recordId, Long version) throws ResourceNotFoundException {
    //if security enabled, check permission -> if not matching, return HTTP UNAUTHORIZED or FORBIDDEN
    long nano = System.nanoTime() / 1000000;
    long nano2;
    Page<DataResource> dataResource;
    try {
      dataResource = metastoreProperties.getDataResourceService().findAllVersions(recordId, null);
    } catch (ResourceNotFoundException ex) {
      ex.setDetail("Metadata document with ID '" + recordId + "' doesn't exist!");
      throw ex;
    }
    nano2 = System.nanoTime() / 1000000;
    Stream<DataResource> stream = dataResource.get();
    if (version != null) {
      stream = stream.filter(resource -> Long.parseLong(resource.getVersion()) == version);
    }
    Optional<DataResource> findFirst = stream.findFirst();
    if (findFirst.isEmpty()) {
      String message = String.format("Version '%d' of ID '%s' doesn't exist!", version, recordId);
      LOG.error(message);
      throw new ResourceNotFoundException(message);
    }
    long nano3 = System.nanoTime() / 1000000;
    LOG.info("getRecordByIdAndVersion {}, {}, {}", nano, (nano2 - nano), (nano3 - nano));
    return findFirst.get();
  }

  public static Path getMetadataDocumentByIdAndVersion(MetastoreConfiguration metastoreProperties,
          String recordId) throws ResourceNotFoundException {
    return getMetadataDocumentByIdAndVersion(metastoreProperties, recordId, null);
  }

  public static Path getMetadataDocumentByIdAndVersion(MetastoreConfiguration metastoreProperties,
          String recordId, Long version) throws ResourceNotFoundException {
    LOG.trace("Obtaining metadata record with id {} and version {}.", recordId, version);
    MetadataRecord metadataRecord = getRecordByIdAndVersion(metastoreProperties, recordId, version);

    URI metadataDocumentUri = URI.create(metadataRecord.getMetadataDocumentUri());

    Path metadataDocumentPath = Paths.get(metadataDocumentUri);
    if (!Files.exists(metadataDocumentPath) || !Files.isRegularFile(metadataDocumentPath) || !Files.isReadable(metadataDocumentPath)) {
      LOG.warn("Metadata document at path {} either does not exist or is no file or is not readable. Returning HTTP NOT_FOUND.", metadataDocumentPath);
      throw new CustomInternalServerError("Metadata document on server either does not exist or is no file or is not readable.");
    }
    return metadataDocumentPath;
  }

  /**
   * Merge new metadata record in the existing one.
   *
   * @param managed Existing metadata record.
   * @param provided New metadata record.
   * @return Merged record
   */
  public static MetadataRecord mergeRecords(MetadataRecord managed, MetadataRecord provided) {
    if (provided != null && managed != null) {
      //update pid
      managed.setPid(mergeEntry("Update record->pid", managed.getPid(), provided.getPid()));

      //update acl
      managed.setAcl(mergeAcl(managed.getAcl(), provided.getAcl()));
      //update getRelatedResource
      managed.setRelatedResource(mergeEntry("Updating record->relatedResource", managed.getRelatedResource(), provided.getRelatedResource()));
      //update schemaId
      managed.setSchema(mergeEntry("Updating record->schema", managed.getSchema(), provided.getSchema()));
      //update schemaVersion
      managed.setSchemaVersion(mergeEntry("Updating record->schemaVersion", managed.getSchemaVersion(), provided.getSchemaVersion()));
      // update licenseUri
      managed.setLicenseUri(mergeEntry("Updating record->licenseUri", managed.getLicenseUri(), provided.getLicenseUri(), true));
    } else {
      managed = (managed != null) ? managed : provided;
    }
    return managed;
  }

  /**
   * Check validity of acl list and then merge new acl list in the existing one.
   *
   * @param managed Existing metadata record.
   * @param provided New metadata record.
   * @return Merged list
   */
  public static Set<AclEntry> mergeAcl(Set<AclEntry> managed, Set<AclEntry> provided) {
    // Check for null parameters (which shouldn't happen)
    managed = (managed == null) ? new HashSet<>() : managed;
    provided = (provided == null) ? new HashSet<>() : provided;
    if (!provided.isEmpty()) {
      if (!provided.equals(managed)) {
        // check for special access rights 
        // - only administrators are allowed to change ACL
        checkAccessRights(managed, true);
        // - at least principal has to remain as ADMIN 
        checkAccessRights(provided, false);
        LOG.trace("Updating record acl from {} to {}.", managed, provided);
        managed = provided;
      } else {
        LOG.trace("Provided ACL is still the same -> Continue using old one.");
      }
    } else {
      LOG.trace("Provided ACL is empty -> Continue using old one.");
    }
    return managed;
  }

  /**
   * Set new value for existing one.
   *
   * @param description For logging purposes only
   * @param managed Existing value.
   * @param provided New value.
   * @return Merged record
   */
  public static <T> T mergeEntry(String description, T managed, T provided) {
    return mergeEntry(description, managed, provided, false);
  }

  /**
   * Set new value for existing one.
   *
   * @param description For logging purposes only
   * @param managed Existing value.
   * @param provided New value.
   * @param overwriteWithNull Allows also deletion of a value.
   * @return Merged record
   */
  public static <T> T mergeEntry(String description, T managed, T provided, boolean overwriteWithNull) {
    if ((provided != null && !provided.equals(managed))
            || overwriteWithNull) {
      LOG.trace(description + " from '{}' to '{}'", managed, provided);
      managed = provided;
    }
    return managed;
  }

  /**
   * Return the number of ingested documents. If there are two versions of the
   * same document this will be counted as two.
   *
   * @return Number of registered documents.
   */
  public static long getNoOfDocuments() {
    return dataRecordDao.count();
  }

  public static void setToken(String bearerToken) {
    guestToken = bearerToken;
  }

  /**
   * Set schema config.
   *
   * @param aSchemaConfig the schemaConfig to set
   */
  public static void setSchemaConfig(MetastoreConfiguration aSchemaConfig) {
    schemaConfig = aSchemaConfig;
  }

  /**
   * Set DAO for data record.
   *
   * @param aDataRecordDao the dataRecordDao to set
   */
  public static void setDataRecordDao(IDataRecordDao aDataRecordDao) {
    dataRecordDao = aDataRecordDao;
  }

  /**
   * Set the DAO for MetadataFormat.
   *
   * @param aMetadataFormatDao the metadataFormatDao to set
   */
  public static void setMetadataFormatDao(IMetadataFormatDao aMetadataFormatDao) {
    metadataFormatDao = aMetadataFormatDao;
  }

  /**
   * Set the DAO for SchemaRecord.
   *
   * @param aSchemaRecordDao the schemaRecordDao to set
   */
  public static void setSchemaRecordDao(ISchemaRecordDao aSchemaRecordDao) {
    schemaRecordDao = aSchemaRecordDao;
  }

  private static void saveNewDataRecord(MetadataRecord result) {
    DataRecord dataRecord;

    // Create shortcut for access.
    LOG.trace("Save new data record!");
    dataRecord = transformToDataRecord(result);

    saveNewDataRecord(dataRecord);
  }

  private static DataRecord transformToDataRecord(MetadataRecord result) {
    DataRecord dataRecord = null;
    if (result != null) {
      LOG.trace("Transform to data record!");
      dataRecord = new DataRecord();
      dataRecord.setMetadataId(result.getId());
      dataRecord.setVersion(result.getRecordVersion());
      dataRecord.setSchemaId(result.getSchema().getIdentifier());
      dataRecord.setSchemaVersion(result.getSchemaVersion());
      dataRecord.setDocumentHash(result.getDocumentHash());
      dataRecord.setMetadataDocumentUri(result.getMetadataDocumentUri());
      dataRecord.setLastUpdate(result.getLastUpdate());
    }
    return dataRecord;
  }

  private static void saveNewDataRecord(DataRecord dataRecord) {
    if (dataRecordDao != null) {
      try {
        dataRecordDao.save(dataRecord);
      } catch (Exception ex) {
        LOG.error("Error saving data record", ex);
      }
      LOG.trace("Data record saved: {}", dataRecord);
    }
  }

  /**
   * Checks if current user is allowed to access with given AclEntries.
   *
   * @param aclEntries AclEntries of resource.
   * @param currentAcl Check current ACL (true) or new one (false).
   *
   * @return Allowed (true) or not.
   */
  public static boolean checkAccessRights(Set<AclEntry> aclEntries, boolean currentAcl) {
    boolean isAllowed = false;
    String errorMessage1 = "Error invalid ACL! Reason: Only ADMINISTRATORS are allowed to change ACL entries.";
    String errorMessage2 = "Error invalid ACL! Reason: You are not allowed to revoke your own administrator rights.";
    Authentication authentication = AuthenticationHelper.getAuthentication();
    List<String> authorizationIdentities = AuthenticationHelper.getAuthorizationIdentities();
    for (GrantedAuthority authority : authentication.getAuthorities()) {
      authorizationIdentities.add(authority.getAuthority());
    }
    if (authorizationIdentities.contains(RepoUserRole.ADMINISTRATOR.getValue())) {
      //ROLE_ADMINISTRATOR detected -> no further permission check necessary.
      return true;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Check access rights for changing ACL list!");
      for (String authority : authorizationIdentities) {
        LOG.trace("Indentity/Authority: '{}'", authority);
      }
    }
    // Check if authorized user still has ADMINISTRATOR rights
    Iterator<AclEntry> iterator = aclEntries.iterator();
    while (iterator.hasNext()) {
      AclEntry aclEntry = iterator.next();
      LOG.trace("'{}' has ’{}' rights!", aclEntry.getSid(), aclEntry.getPermission());
      if (aclEntry.getPermission().atLeast(PERMISSION.ADMINISTRATE)
              && authorizationIdentities.contains(aclEntry.getSid())) {
        isAllowed = true;
        LOG.trace("Confirm permission for updating ACL: '{}' has ’{}' rights!", aclEntry.getSid(), PERMISSION.ADMINISTRATE);
        break;
      }
    }
    if (!isAllowed) {
      String errorMessage = currentAcl ? errorMessage1 : errorMessage2;
      LOG.warn(errorMessage);
      if (schemaConfig.isAuthEnabled()) {
        if (currentAcl) {
          throw new AccessForbiddenException(errorMessage1);
        } else {
          throw new BadArgumentException(errorMessage2);
        }
      }
    }
    return isAllowed;
  }

  public static final void fixMetadataDocumentUri(MetadataRecord metadataRecord) {
    String metadataDocumentUri = metadataRecord.getMetadataDocumentUri();
    metadataRecord
            .setMetadataDocumentUri(WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(MetadataControllerImpl.class
            ).getMetadataDocumentById(metadataRecord.getId(), metadataRecord.getRecordVersion(), null, null)).toUri().toString());
    LOG.trace("Fix metadata document Uri '{}' -> '{}'", metadataDocumentUri, metadataRecord.getMetadataDocumentUri());
  }

  public static void checkLicense(DataResource dataResource, String licenseUri) {
    if (licenseUri != null) {
      Set<Scheme> rights = dataResource.getRights();
      String licenseId = licenseUri.substring(licenseUri.lastIndexOf("/"));
      Scheme license = Scheme.factoryScheme(licenseId, licenseUri);
      if (rights.isEmpty()) {
        rights.add(license);
      } else {
        // Check if license already exists (only one license allowed)
        if (!rights.contains(license)) {
          rights.clear();
          rights.add(license);
        }
      }
    } else {
      // Remove license
      dataResource.getRights().clear();
    }
  }

  public static void check4RelatedResource(DataResource dataResource, RelatedIdentifier relatedResource) {
    if (relatedResource != null) {
      Set<RelatedIdentifier> relatedResources = dataResource.getRelatedIdentifiers();

      if (relatedResources.isEmpty()) {
        relatedResources.add(relatedResource);
      } else {
        // Check if related resource already exists (only one related resource of each type allowed)
        for (RelatedIdentifier item : relatedResources) {
          if (item.getRelationType().equals(relatedResource.getRelationType())
                  && !item.getValue().equals(relatedResource.getValue())) {
            relatedResources.remove(item);
            relatedResources.add(relatedResource);
            break;
          }
        }
      }
    }
  }

  public static void validateRelatedResources4MetadataDocuments(DataResource dataResource) throws BadArgumentException {
    int noOfRelatedData = 0;
    int noOfRelatedSchemas = 0;
    String message = "Invalid related resources! Expected '1' related resource found '%d'. Expected '1' related schema found '%d'!";
    if (dataResource != null) {
      Set<RelatedIdentifier> relatedResources = dataResource.getRelatedIdentifiers();

      // Check if related resource already exists (only one related resource of type isMetadataFor allowed)
      for (RelatedIdentifier item : relatedResources) {
        if (item.getRelationType().equals(RelatedIdentifier.RELATION_TYPES.IS_METADATA_FOR)) {
          noOfRelatedData++;
        }
        if (item.getRelationType().equals(RelatedIdentifier.RELATION_TYPES.IS_DERIVED_FROM)) {
          noOfRelatedSchemas++;
        }
      }
    }
    if (noOfRelatedData != 1 || noOfRelatedSchemas != 1) {
      String errorMessage = "";
      if (noOfRelatedData == 0) {
        errorMessage = "Mandatory attribute relatedIdentifier of type 'isMetadataFor' was not found in record. \n";
      }
      if (noOfRelatedData > 1) {
        errorMessage = "Mandatory attribute relatedIdentifier of type 'isMetadataFor' was provided more than once in record. \n";
      }
      if (noOfRelatedSchemas == 0) {
        errorMessage = errorMessage + "Mandatory attribute relatedIdentifier of type 'isDerivedFrom' was not found in record. \n";
      }
      if (noOfRelatedSchemas > 1) {
        errorMessage = errorMessage + "Mandatory attribute relatedIdentifier of type 'isDerivedFrom' was provided more than once in record. \n";
      }
      errorMessage = errorMessage + "Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(errorMessage);
    }
  }

  /**
   * Transform schema identifier to global available identifier (if neccessary).
   *
   * @param dataResourceRecord Metadata record hold schema identifier.
   * @return ResourceIdentifier with a global accessible identifier.
   */
  public static RelatedIdentifier getSchemaIdentifier(DataResource dataResourceRecord) {
    LOG.trace("Get schema identifier for '{}'.", dataResourceRecord.getId());
    RelatedIdentifier schemaIdentifier = null;

    Set<RelatedIdentifier> relatedResources = dataResourceRecord.getRelatedIdentifiers();

    // Check if related resource already exists (only one related resource of type isMetadataFor allowed)
    for (RelatedIdentifier item : relatedResources) {
      if (item.getRelationType().equals(RelatedIdentifier.RELATION_TYPES.IS_DERIVED_FROM)) {
        schemaIdentifier = item;
      }
    }
    return schemaIdentifier;
  }

  /**
   * Check if ID for schema is valid. Requirements: - shouldn't change if URL
   * encoded - should be lower case If it's not lower case the original ID will
   * we set as an alternate ID.
   *
   * @param metadataRecord Datacite Record.
   */
  public static final void check4validSchemaId(DataResource metadataRecord) {
    check4validId(metadataRecord);
    String id = metadataRecord.getId();
    String lowerCaseId = id.toLowerCase();
    // schema id should be lower case due to elasticsearch
    if (!lowerCaseId.equals(id)) {
      metadataRecord.getAlternateIdentifiers().add(Identifier.factoryInternalIdentifier(id));
      metadataRecord.setId(lowerCaseId);
    }
  }

  public static final void check4validId(DataResource metadataRecord) {
    try {
      String value = URLEncoder.encode(metadataRecord.getId(), StandardCharsets.UTF_8.toString());
      if (!value.equals(metadataRecord.getId())) {
        String message = "Not a valid schema id! Encoded: " + value;
        LOG.error(message);
        throw new BadArgumentException(message);
      }
    } catch (UnsupportedEncodingException ex) {
      String message = "Error encoding schemaId " + metadataRecord.getId();
      LOG.error(message);
      throw new CustomInternalServerError(message);
    }
  }

  private static void validateMetadataSchemaDocument(MetastoreConfiguration metastoreProperties, SchemaRecord schemaRecord, MultipartFile document) {
    LOG.debug("Validate metadata schema document...");
    if (document == null || document.isEmpty()) {
      String message = "Missing metadata schema document in body. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    try {
      validateMetadataSchemaDocument(metastoreProperties, schemaRecord, document.getBytes());
    } catch (IOException ex) {
      String message = LOG_ERROR_READ_METADATA_DOCUMENT;
      LOG.error(message, ex);
      throw new UnprocessableEntityException(message);
    }
  }

  private static void validateMetadataSchemaDocument(MetastoreConfiguration metastoreProperties, SchemaRecord schemaRecord, byte[] document) {
    LOG.debug("Validate metadata schema document...");
    if (document == null || document.length == 0) {
      String message = "Missing metadata schema document in body. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }

    IValidator applicableValidator = null;
    try {
      applicableValidator = getValidatorForRecord(metastoreProperties, schemaRecord, document);

      if (applicableValidator == null) {
        String message = "No validator found for schema type " + schemaRecord.getType() + ". Returning HTTP UNPROCESSABLE_ENTITY.";
        LOG.error(message);
        throw new UnprocessableEntityException(message);
      } else {
        LOG.trace("Validator found. Checking provided schema file.");
        LOG.trace("Performing validation of metadata document using schema {}, version {} and validator {}.", schemaRecord.getSchemaId(), schemaRecord.getVersion(), applicableValidator);
        try (InputStream inputStream = new ByteArrayInputStream(document)) {
          if (!applicableValidator.isSchemaValid(inputStream)) {
            String message = "Metadata schema document validation failed. Returning HTTP UNPROCESSABLE_ENTITY.";
            LOG.warn(message);
            if (LOG.isTraceEnabled()) {
              LOG.trace("Schema: \n'{}'", new String(document, StandardCharsets.UTF_8));
            }
            throw new UnprocessableEntityException(message);
          }
        }
      }
    } catch (IOException ex) {
      String message = LOG_ERROR_READ_METADATA_DOCUMENT;
      LOG.error(message, ex);
      throw new UnprocessableEntityException(message);
    }

    LOG.trace("Schema document is valid!");
  }

  private static IValidator getValidatorForRecord(MetastoreConfiguration metastoreProperties, SchemaRecord schemaRecord, byte[] schemaDocument) {
    IValidator applicableValidator = null;
    //obtain/guess record type
    if (schemaRecord.getType() == null) {
      schemaRecord.setType(SchemaUtils.guessType(schemaDocument));
      if (schemaRecord.getType() == null) {
        String message = "Unable to detect schema type automatically. Please provide a valid type";
        LOG.error(message);
        throw new UnprocessableEntityException(message);
      } else {
        LOG.debug("Automatically detected schema type {}.", schemaRecord.getType());
      }
    }
    for (IValidator validator : metastoreProperties.getValidators()) {
      if (validator.supportsSchemaType(schemaRecord.getType())) {
        applicableValidator = validator.getInstance();
        LOG.trace("Found validator for schema: '{}'", schemaRecord.getType().name());
        break;
      }
    }
    return applicableValidator;
  }

  private static void saveNewSchemaRecord(SchemaRecord schemaRecord) {
    if (schemaRecordDao != null) {
      try {
        schemaRecordDao.save(schemaRecord);
      } catch (Exception npe) {
        LOG.error("Can't save schema record: " + schemaRecord, npe);
      }
      LOG.trace("Schema record saved: {}", schemaRecord);
    }
  }

  private static DataResource checkParameters(MultipartFile dataResourceRecord, MultipartFile document, boolean bothRequired) {
    boolean recordAvailable;
    boolean documentAvailable;
    DataResource metadataRecord = null;
    
    recordAvailable = dataResourceRecord == null || dataResourceRecord.isEmpty();
    documentAvailable = document == null || document.isEmpty();
    String message = null;
    if (bothRequired) {
      if (!recordAvailable || !documentAvailable) {
        message = "No data resource record and/or metadata document provided. Returning HTTP BAD_REQUEST.";
      }
    } else {
      if (!(recordAvailable || documentAvailable)) {
        message = "Neither metadata record nor metadata document provided.";
      }
    }
    if (message != null) {
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    // Do some checks first.
    if (!recordAvailable) {
      try {
        metadataRecord = Json.mapper().readValue(dataResourceRecord.getInputStream(), DataResource.class);
      } catch (IOException ex) {
        message = "Can't map record document to MetadataRecord";
        if (ex instanceof JsonParseException) {
          message = message + " Reason: " + ex.getMessage();
        }
        LOG.error(ERROR_PARSING_JSON, ex);
        throw new BadArgumentException(message);
      }
    }
    return metadataRecord;
  }

  /**
   * Validate metadata document with given schema. In case of an error a runtime
   * exception is thrown.
   *
   * @param metastoreProperties Configuration properties.
   * @param document Document to validate.
   * @param schemaId SchemaId of schema.
   * @param version Version of the document.
   */
  public static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
          MultipartFile document,
          String schemaId,
          Long version) {
    LOG.trace("validateMetadataDocument {},{}, {}", metastoreProperties, schemaId, document);
    if (schemaId == null) {
      String message = "Missing schemaID. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }

    long nano1 = System.nanoTime() / 1000000;
    ResourceIdentifier resourceIdentifier = ResourceIdentifier.factoryInternalResourceIdentifier(schemaId);
    SchemaRecord schemaRecord = getSchemaRecord(resourceIdentifier, version);
    long nano2 = System.nanoTime() / 1000000;
    validateMetadataDocument(metastoreProperties, document, schemaRecord);
    long nano3 = System.nanoTime() / 1000000;
    LOG.info("Validate document(schemaId,version), {}, {}, {}", nano1, nano2 - nano1, nano3 - nano1);
    
    cleanUp(schemaRecord);
  }

  /**
   * Validate metadata document with given schema. In case of an error a runtime
   * exception is thrown.
   *
   * @param metastoreProperties Configuration properties.
   * @param document Document to validate.
   * @param schemaRecord Record of the schema.
   */
  public static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
          MultipartFile document,
          SchemaRecord schemaRecord) {
    LOG.trace("validateMetadataDocument {},{}, {}", metastoreProperties, schemaRecord, document);

    if (document == null || document.isEmpty()) {
      String message = "Missing metadata document in body. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    try {
      try (InputStream inputStream = document.getInputStream()) {
        validateMetadataDocument(metastoreProperties, inputStream, schemaRecord);
      }
    } catch (IOException ex) {
      String message = LOG_ERROR_READ_METADATA_DOCUMENT;
      LOG.error(message, ex);
      throw new UnprocessableEntityException(message);
    }
  }

  /**
   * Validate metadata document with given schema. In case of an error a runtime
   * exception is thrown.
   *
   * @param metastoreProperties Configuration properties.
   * @param inputStream Document to validate.
   * @param schemaRecord Record of the schema.
   */
  public static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
          InputStream inputStream,
          SchemaRecord schemaRecord) throws IOException {
    LOG.trace("validateMetadataInputStream {},{}, {}", metastoreProperties, schemaRecord, inputStream);

    long nano1 = System.nanoTime() / 1000000;
    if (schemaRecord == null || schemaRecord.getSchemaDocumentUri() == null || schemaRecord.getSchemaDocumentUri().trim().isEmpty()) {
      String message = "Missing or invalid schema record. Returning HTTP BAD_REQUEST.";
      LOG.error(message + " -> '{}'", schemaRecord);
      throw new BadArgumentException(message);
    }
    long nano2 = System.nanoTime() / 1000000;
    LOG.trace("Checking local schema file.");
    Path schemaDocumentPath = Paths.get(URI.create(schemaRecord.getSchemaDocumentUri()));

    if (!Files.exists(schemaDocumentPath) || !Files.isRegularFile(schemaDocumentPath) || !Files.isReadable(schemaDocumentPath)) {
      LOG.error("Schema document with schemaId '{}'at path {} either does not exist or is no file or is not readable.", schemaRecord.getSchemaId(), schemaDocumentPath);
      throw new CustomInternalServerError("Schema document on server either does not exist or is no file or is not readable.");
    }
    LOG.trace("obtain validator for type");
    IValidator applicableValidator;
    if (schemaRecord.getType() == null) {
      byte[] schemaDocument = FileUtils.readFileToByteArray(schemaDocumentPath.toFile());
      applicableValidator = getValidatorForRecord(metastoreProperties, schemaRecord, schemaDocument);
    } else {
      applicableValidator = getValidatorForRecord(metastoreProperties, schemaRecord, null);
    }
    long nano3 = System.nanoTime() / 1000000;

    if (applicableValidator == null) {
      String message = "No validator found for schema type " + schemaRecord.getType();
      LOG.error(message);
      throw new UnprocessableEntityException(message);
    } else {
      LOG.trace("Validator found.");

      LOG.trace("Performing validation of metadata document using schema {}, version {} and validator {}.", schemaRecord.getSchemaId(), schemaRecord.getVersion(), applicableValidator);
      long nano4 = System.nanoTime() / 1000000;
      if (!applicableValidator.validateMetadataDocument(schemaDocumentPath.toFile(), inputStream)) {
        LOG.warn("Metadata document validation failed. -> " + applicableValidator.getErrorMessage());
        throw new UnprocessableEntityException(applicableValidator.getErrorMessage());
      }
      long nano5 = System.nanoTime() / 1000000;
      LOG.info("Validate document(schemaRecord), {}, {}, {}, {}, {}, {}", nano1, nano2 - nano1, nano3 - nano1, nano4 - nano1, nano5 - nano1);
    }
    LOG.trace("Metadata document validation succeeded.");
  }

  /**
   * Gets SchemaRecord from identifier. Afterwards there should be a clean up.
   *
   * @see #cleanUp(edu.kit.datamanager.metastore2.domain.ResourceIdentifier,
   * edu.kit.datamanager.metastore2.domain.SchemaRecord)
   *
   * @param identifier ResourceIdentifier of type INTERNAL or URL.
   * @param version Version (may be null)
   * @return schema record.
   */
  public static SchemaRecord getSchemaRecord(ResourceIdentifier identifier, Long version) {
    LOG.trace("getSchemaRecord {},{}", identifier, version);
    SchemaRecord schemaRecord;
    if (identifier == null || identifier.getIdentifierType() == null) {
      String message = "Missing resource identifier for schema. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    switch (identifier.getIdentifierType()) {
      case INTERNAL -> {
        String schemaId = identifier.getIdentifier();
        if (schemaId == null) {
          String message = "Missing schemaID. Returning HTTP BAD_REQUEST.";
          LOG.error(message);
          throw new BadArgumentException(message);
        }
        if (version != null) {
          schemaRecord = schemaRecordDao.findBySchemaIdAndVersion(schemaId, version);
        } else {
          schemaRecord = schemaRecordDao.findFirstBySchemaIdOrderByVersionDesc(schemaId);
        }
      }
      case URL -> {
        schemaRecord = prepareResourceFromUrl(identifier, version);
      }
      default -> throw new BadArgumentException("For schema document identifier type '" + identifier.getIdentifierType() + "' is not allowed!");
    }
    if (schemaRecord != null) {
      LOG.trace("getSchemaRecord {},{}", schemaRecord.getSchemaDocumentUri(), schemaRecord.getVersion());
    } else {
      LOG.trace("No matching schema record found!");
    }
    return schemaRecord;
  }

  private static SchemaRecord prepareResourceFromUrl(ResourceIdentifier identifier, Long version) {
            String url = identifier.getIdentifier();
        Path pathToFile;
        MetadataSchemaRecord.SCHEMA_TYPE type = null;
        Optional<Url2Path> findByUrl = url2PathDao.findByUrl(url);
        if (findByUrl.isPresent()) {
          url = findByUrl.get().getPath();
          type = findByUrl.get().getType();
          pathToFile = Paths.get(URI.create(url));
        } else {
          URI resourceUrl;
          try {
            resourceUrl = new URI(url);
          } catch (URISyntaxException ex) {
            String message = String.format("Invalid URL: '%s'", url);
            LOG.error(message, ex);
            throw new BadArgumentException(message);
          }
          Optional<Path> path = DownloadUtil.downloadResource(resourceUrl);
          pathToFile = path.get();
        }
        SchemaRecord schemaRecord = new SchemaRecord();
        schemaRecord.setSchemaDocumentUri(pathToFile.toUri().toString());
        schemaRecord.setType(type);
     return schemaRecord;
  }
  /**
   * Remove all downloaded files for schema Record.
   *
   * @param schemaRecord Schema record.
   */
  public static void cleanUp(SchemaRecord schemaRecord) {
    LOG.trace("Clean up {}", schemaRecord);
    if (schemaRecord == null || schemaRecord.getSchemaDocumentUri() == null) {
      String message = "Missing resource locator for schema.";
      LOG.error(message);
    } else {
      String pathToSchemaDocument = fixRelativeURI(schemaRecord.getSchemaDocumentUri());
      List<Url2Path> findByUrl = url2PathDao.findByPath(pathToSchemaDocument);
      if (findByUrl.isEmpty()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(LOG_SEPARATOR);
          Page<Url2Path> page = url2PathDao.findAll(PageRequest.of(0, 100));
          LOG.trace("List '{}' of '{}'", page.getSize(), page.getTotalElements());
          LOG.trace(LOG_SEPARATOR);
          page.getContent().forEach(item -> LOG.trace("- {}", item));
          LOG.trace(LOG_SEPARATOR);
        }
        // Remove downloaded file
        String uri = schemaRecord.getSchemaDocumentUri();
        Path pathToFile = Paths.get(URI.create(uri));
        DownloadUtil.removeFile(pathToFile);
      }
    }
  }

  /**
   * Set the DAO holding url and paths.
   *
   * @param aUrl2PathDao the url2PathDao to set
   */
  public static void setUrl2PathDao(IUrl2PathDao aUrl2PathDao) {
    url2PathDao = aUrl2PathDao;
  }

  /**
   * Update schema document.
   *
   * @param applicationProperties Settings of repository.
   * @param resourceId ID of the schema document.
   * @param eTag E-Tag of the current schema document.
   * @param recordDocument Record of the schema.
   * @param schemaDocument Schema document.
   * @param supplier Method for creating access URL.
   * @return Record of updated schema document.
   */
  public static DataResource updateMetadataSchemaRecord(MetastoreConfiguration applicationProperties,
          String resourceId,
          String eTag,
          MultipartFile recordDocument,
          MultipartFile schemaDocument,
          UnaryOperator<String> supplier) {
    DataResource metadataRecord;
    metadataRecord = checkParameters(recordDocument, schemaDocument, false);

    LOG.trace("Obtaining most recent metadata schema record with id {}.", resourceId);
    DataResource dataResource = applicationProperties.getDataResourceService().findById(resourceId);
    LOG.trace("Checking provided ETag.");
    ControllerUtils.checkEtag(eTag, dataResource);
    SchemaRecord schemaRecord = schemaRecordDao.findFirstBySchemaIdOrderByVersionDesc(dataResource.getId());
    if (metadataRecord != null) {
      metadataRecord.setVersion(Long.toString(schemaRecord.getVersion()));
      existingRecord = mergeRecords(existingRecord, metadataRecord);
      mergeSchemaRecord(schemaRecord, existingRecord);
      dataResource = migrateToDataResource(applicationProperties, existingRecord);
    } else {
      dataResource = DataResourceUtils.copyDataResource(dataResource);
    }

    if (schemaDocument != null) {
      // Get schema record for this schema
      validateMetadataSchemaDocument(applicationProperties, schemaRecord, schemaDocument);

      ContentInformation info;
      info = getContentInformationOfResource(applicationProperties, dataResource);

      boolean noChanges = false;
      String fileName = schemaDocument.getOriginalFilename();
      if (info != null) {
        noChanges = true;
        fileName = info.getRelativePath();
        // Check for changes...
        try {
          byte[] currentFileContent;
          File file = new File(URI.create(info.getContentUri()));
          if (schemaDocument.getSize() == Files.size(file.toPath())) {
            currentFileContent = FileUtils.readFileToByteArray(file);
            byte[] newFileContent = schemaDocument.getBytes();
            for (int index = 0; index < currentFileContent.length; index++) {
              if (currentFileContent[index] != newFileContent[index]) {
                noChanges = false;
                break;
              }
            }
          } else {
            noChanges = false;
          }
        } catch (IOException ex) {
          LOG.error("Error reading current file!", ex);
          throw new BadArgumentException("Error reading schema document!");
        }
      }
      if (!noChanges) {
        // Everything seems to be fine update document and increment version
        LOG.trace("Updating schema document (and increment version)...");
        String version = dataResource.getVersion();
        if (version != null) {
          dataResource.setVersion(Long.toString(Long.parseLong(version) + 1l));
        }
        ContentDataUtils.addFile(applicationProperties, dataResource, schemaDocument, fileName, null, true, supplier);
      } else {
        schemaRecordDao.delete(schemaRecord);
      }
    } else {
      schemaRecordDao.delete(schemaRecord);
      // validate if document is still valid due to changed record settings.
      metadataRecord = migrateToMetadataSchemaRecord(applicationProperties, dataResource, false);
      URI schemaDocumentUri = URI.create(metadataRecord.getSchemaDocumentUri());

      Path schemaDocumentPath = Paths.get(schemaDocumentUri);
      if (!Files.exists(schemaDocumentPath) || !Files.isRegularFile(schemaDocumentPath) || !Files.isReadable(schemaDocumentPath)) {
        LOG.warn("Schema document at path {} either does not exist or is no file or is not readable. Returning HTTP NOT_FOUND.", schemaDocumentPath);
        throw new CustomInternalServerError("Schema document on server either does not exist or is no file or is not readable.");
      }

      try {
        byte[] schemaDoc = Files.readAllBytes(schemaDocumentPath);
        MetadataSchemaRecordUtil.validateMetadataSchemaDocument(applicationProperties, schemaRecord, schemaDoc);
      } catch (IOException ex) {
        LOG.error("Error validating file!", ex);
      }

    }
    dataResource = DataResourceUtils.updateResource(applicationProperties, resourceId, dataResource, eTag, supplier);

    return migrateToMetadataSchemaRecord(applicationProperties, dataResource, true);
  }


}