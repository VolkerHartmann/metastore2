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
import edu.kit.datamanager.entities.Identifier;
import edu.kit.datamanager.entities.PERMISSION;
import edu.kit.datamanager.entities.RepoUserRole;
import edu.kit.datamanager.exceptions.*;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.IResource2FileVersionDao;
import edu.kit.datamanager.metastore2.dao.ISchemaUrl2PathDao;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import edu.kit.datamanager.metastore2.domain.Resource2FileVersion;
import edu.kit.datamanager.metastore2.domain.SchemaUrl2Path;
import edu.kit.datamanager.metastore2.validation.IValidator;
import edu.kit.datamanager.metastore2.web.impl.MetadataControllerImplV2;
import edu.kit.datamanager.metastore2.web.impl.SchemaRegistryControllerImplV2;
import edu.kit.datamanager.repo.configuration.RepoBaseConfiguration;
import edu.kit.datamanager.repo.dao.IAllIdentifiersDao;
import edu.kit.datamanager.repo.dao.IDataResourceDao;
import edu.kit.datamanager.repo.dao.spec.dataresource.*;
import edu.kit.datamanager.repo.domain.*;
import edu.kit.datamanager.repo.domain.Date;
import edu.kit.datamanager.repo.domain.acl.AclEntry;
import edu.kit.datamanager.repo.service.IContentInformationService;
import edu.kit.datamanager.repo.util.ContentDataUtils;
import edu.kit.datamanager.repo.util.DataResourceUtils;
import edu.kit.datamanager.util.AuthenticationHelper;
import edu.kit.datamanager.util.ControllerUtils;
import io.swagger.v3.core.util.Json;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord.SCHEMA_TYPE.JSON;
import static edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord.SCHEMA_TYPE.XML;

/**
 * Utility class for handling json documents
 */
public class DataResourceRecordUtil {

  public static final String RESOURCE_TYPE = "application/vnd.datacite.org+json";

  public static final RelatedIdentifier.RELATION_TYPES RELATED_DATA_RESOURCE_TYPE = RelatedIdentifier.RELATION_TYPES.IS_METADATA_FOR;
  public static final RelatedIdentifier.RELATION_TYPES RELATED_SCHEMA_TYPE = RelatedIdentifier.RELATION_TYPES.HAS_METADATA;
  public static final RelatedIdentifier.RELATION_TYPES RELATED_NEW_VERSION_OF = RelatedIdentifier.RELATION_TYPES.IS_NEW_VERSION_OF;
  /**
   * Mediatype for fetching a DataResource.
   */
  public static final MediaType DATA_RESOURCE_MEDIA_TYPE = MediaType.valueOf(RESOURCE_TYPE);

  /**
   * Separator for separating schemaId and schemaVersion.
   */
  public static final String SCHEMA_VERSION_SEPARATOR = "/";
  /**
   * Logger for messages.
   */
  private static final Logger LOG = LoggerFactory.getLogger(DataResourceRecordUtil.class);

  private static final String LOG_ERROR_READ_METADATA_DOCUMENT = "Failed to read metadata document from input stream.";
  private static final String ERROR_PARSING_JSON = "Error parsing json: ";

  private static MetastoreConfiguration schemaConfig;

  private static IDataResourceDao dataResourceDao;
  private static ISchemaUrl2PathDao schemaUrl2PathDao;
  private static IResource2FileVersionDao resource2FileVersionDao;
  private static IAllIdentifiersDao allIdentifiersDao;

  public static final String SCHEMA_SUFFIX = "_Schema";
  public static final String XML_SCHEMA_TYPE = MetadataSchemaRecord.SCHEMA_TYPE.XML + SCHEMA_SUFFIX;
  public static final String JSON_SCHEMA_TYPE = MetadataSchemaRecord.SCHEMA_TYPE.JSON + SCHEMA_SUFFIX;

  public static final String METADATA_SUFFIX = "_Metadata";
  public static final String XML_METADATA_TYPE = MetadataSchemaRecord.SCHEMA_TYPE.XML + METADATA_SUFFIX;
  public static final String JSON_METADATA_TYPE = MetadataSchemaRecord.SCHEMA_TYPE.JSON + METADATA_SUFFIX;

  private static String baseUrl;

  private static String guestToken = null;

  DataResourceRecordUtil() {
    //Utility class
  }

  /**
   * Checks if current user is allowed to access with given AclEntries.
   *
   * @param aclEntries AclEntries of resource.
   * @param currentAcl Check current ACL (true) or new one (false).
   * @return Allowed (true) or not.
   */
  public static boolean checkAccessRights(Set<AclEntry> aclEntries, boolean currentAcl) {
    boolean isAllowed = true;
    String errorMessage1 = "Error invalid ACL! Reason: Only ADMINISTRATORS are allowed to change ACL entries.";
    String errorMessage2 = "Error invalid ACL! Reason: You are not allowed to revoke your own administrator rights.";
    List<String> authorizationIdentities = getAllAuthorizationIdentities();
    // No authentication enabled or 
    //ROLE_ADMINISTRATOR detected -> no further permission check necessary.
    if (schemaConfig.isAuthEnabled() && !authorizationIdentities.contains(RepoUserRole.ADMINISTRATOR.getValue())) {
      isAllowed = false;
      // Check if authorized user still has ADMINISTRATOR rights
      for (AclEntry aclEntry : aclEntries) {
        LOG.trace("'{}' has '{}' rights!", aclEntry.getSid(), aclEntry.getPermission());
        if (aclEntry.getPermission().atLeast(PERMISSION.ADMINISTRATE) && authorizationIdentities.contains(aclEntry.getSid())) {
          isAllowed = true;
          LOG.trace("Confirm permission for updating ACL: '{}' has '{}' rights!", aclEntry.getSid(), PERMISSION.ADMINISTRATE);
          break;
        }
      }
      if (!isAllowed) {
        if (currentAcl) {
          LOG.warn(errorMessage1);
          throw new AccessForbiddenException(errorMessage1);
        } else {
          LOG.warn(errorMessage2);
          throw new BadArgumentException(errorMessage2);
        }
      }
    }
    return isAllowed;
  }

  /**
   * Create/Ingest an instance of MetadataSchemaRecord.
   *
   * @param applicationProperties Settings of repository.
   * @param recordDocument        Record of the schema.
   * @param document              Schema document.
   * @return Record of registered schema document.
   */
  public static DataResource createDataResourceRecord4Schema(MetastoreConfiguration applicationProperties,
                                                             MultipartFile recordDocument,
                                                             MultipartFile document) {
    DataResource dataResourceRecord;

    // Do some checks first.
    dataResourceRecord = checkParameters(recordDocument, document, true);
    Objects.requireNonNull(dataResourceRecord);
    if (dataResourceRecord.getId() == null) {
      String message = "Mandatory attribute 'id' not found in record. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    // Check if id is lower case and URL encodable.
    // and save as alternate identifier. (In case of
    // upper letters in both versions (with and without
    // upper letters)
    DataResourceRecordUtil.check4validSchemaId(dataResourceRecord);
    // End of parameter checks
    // validate schema document / determine type if not given
    validateMetadataSchemaDocument(applicationProperties, dataResourceRecord, document);
    // set internal parameters
    if (dataResourceRecord.getResourceType() == null) {
      LOG.trace("No mimetype set! Try to determine...");
      if (document.getContentType() != null) {
        LOG.trace("Set mimetype determined from document: '{}'", document.getContentType());
        dataResourceRecord.getFormats().add(document.getContentType());
      }
    }
    dataResourceRecord.setVersion(SemanticVersion.parse("1.0.0").toString());
    // create record.
    DataResource dataResource = dataResourceRecord;
    DataResource createResource = DataResourceUtils.createResource(applicationProperties, dataResource);
    // store document
    ContentInformation contentInformation = ContentDataUtils.addFile(applicationProperties, createResource, document, document.getOriginalFilename(), null, true, t -> "somethingStupid");
    // Create schema record
    SchemaUrl2Path schemaRecord = createSchemaRecord(dataResource, contentInformation);
    DataResourceRecordUtil.saveNewSchemaRecord(schemaRecord);

    // reload data resource
    dataResourceRecord = DataResourceRecordUtil.getSchemaRecordByIdAndVersion(applicationProperties, dataResourceRecord.getId(), dataResourceRecord.getVersion());

    //save helper table to link file versions to resource
    saveNewResource2FileVersion(dataResourceRecord, contentInformation);

    return dataResourceRecord;
  }

  /**
   * Create/Ingest an instance of MetadataRecord.
   *
   * @param applicationProperties Settings of repository.
   * @param recordDocument        Record of the metadata.
   * @param document              Schema document.
   * @return Record of registered schema document.
   */
  public static DataResource createDataResourceRecord4Metadata(MetastoreConfiguration applicationProperties,
                                                               MultipartFile recordDocument,
                                                               MultipartFile document) {
    DataResource dataResource;

    // Do some checks first.
    dataResource = checkParameters(recordDocument, document, true);
    Objects.requireNonNull(dataResource);
    if (dataResource.getId() != null) {
      // Optional id set. Check for valid ID
      check4validId(dataResource, true);
    }
    // End of parameter checks
    // Fix internal references, of necessary
    fixRelatedSchemaIfNeeded(dataResource);
    // validate schema document / determine or correct resource type
    validateMetadataDocument(applicationProperties, document, dataResource);

    dataResource.setVersion(getSchemaRecordFromDataResource(dataResource).getVersion().toString());
    // create record.
    DataResource createResource = DataResourceUtils.createResource(applicationProperties, dataResource);
    // store document
    ContentInformation contentInformation = ContentDataUtils.addFile(applicationProperties, createResource, document, document.getOriginalFilename(), null, true, t -> "somethingStupid");
    dataResource = DataResourceRecordUtil.getMetadataRecordByIdAndVersion(applicationProperties, dataResource.getId(), dataResource.getVersion());

    //save helper table to link file versions to resource
    saveNewResource2FileVersion(dataResource, contentInformation);

    return dataResource;
  }

  /**
   * Update a digital object with given metadata record and/or metadata
   * document.
   *
   * @param applicationProperties Configuration properties.
   * @param resourceId            Identifier of digital object.
   * @param eTag                  ETag of the old digital object.
   * @param recordDocument        Metadata record.
   * @param document              Metadata document.
   * @param supplier              Function for updating record.
   * @return Enriched metadata record.
   */
  public static DataResource updateDataResource4MetadataDocument(MetastoreConfiguration applicationProperties,
                                                                 String resourceId,
                                                                 String eTag,
                                                                 MultipartFile recordDocument,
                                                                 MultipartFile document,
                                                                 UnaryOperator<String> supplier) {
    DataResource givenDataResource;
    givenDataResource = checkParameters(recordDocument, document, false);

    return updateDataResource4MetadataDocument(applicationProperties, resourceId, eTag, givenDataResource, document, supplier);
  }

  public static DataResource updateDataResource4MetadataDocument(MetastoreConfiguration applicationProperties,
                                                                 String resourceId,
                                                                 String eTag,
                                                                 DataResource givenDataResource,
                                                                 MultipartFile document,
                                                                 UnaryOperator<String> supplier) {
    DataResource updatedDataResource;

    LOG.trace("Obtaining most recent datacite record with id {}.", resourceId);
    DataResource oldDataResource = applicationProperties.getDataResourceService().findById(resourceId);
    ControllerUtils.checkEtag(eTag, oldDataResource);
    LOG.trace("ETag: '{}'", oldDataResource.getEtag());
    DataResource mergedDataResource = mergeDataResource(oldDataResource, givenDataResource);
    updatedDataResource = fixRelatedSchemaIfNeeded(mergedDataResource);

    if (document != null) {
      updateMetadataDocument(applicationProperties, updatedDataResource, document, supplier);
    } else {
      ContentInformation info;
      info = getContentInformationOfResource(applicationProperties, updatedDataResource);
      // validate if document is still valid due to changed record settings.
      if (info != null) {
        // version should not be changed, but check if document is still valid for updated record.
        updatedDataResource.setVersion(oldDataResource.getVersion());
        Path metadataDocumentPath = testForRegularFile(info.getContentUri());
        // test if document is still valid for updated(?) schema.
        try {
          InputStream inputStream = Files.newInputStream(metadataDocumentPath);
          SchemaUrl2Path schemaRecord = DataResourceRecordUtil.getSchemaRecordFromDataResource(updatedDataResource);
          SchemaRecordUtil.validateMetadataDocument(applicationProperties, inputStream, schemaRecord);
        } catch (IOException ex) {
          LOG.error("Error validating file!", ex);
        }
      } else {
        throw new CustomInternalServerError("Metadata document on server does not exist!");
      }

    }
    oldDataResource = DataResourceUtils.updateResource(applicationProperties, resourceId, updatedDataResource, eTag, supplier);

    return oldDataResource;
  }

  /**
   * Add or replace link to predecessor.
   *
   * @param newDataResource Data resource holding the new version.
   */
  public static void addProvenance(DataResource newDataResource) {
    if (SemanticVersion.parse(newDataResource.getVersion()).isAfter(SemanticVersion.parse("1.0.0"))) {
      replaceIsDerivedFrom(newDataResource);
    }
  }

  /**
   * Replace outdated link to predecessor with new one.
   *
   * @param newDataResource Data resource holding the new version.
   */
  public static void replaceIsDerivedFrom(DataResource newDataResource) {
    boolean foundOldIdentifier = false;
    String oldVersion = DataResourceRecordUtil.getPreviousVersion(newDataResource);
    String urlToPredecessor = WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(MetadataControllerImplV2.class).
                    getMetadataDocumentById(newDataResource.getId(), oldVersion, null, null)).
            toUri().
            toString();
    for (RelatedIdentifier item : newDataResource.getRelatedIdentifiers()) {
      if (item.getRelationType().equals(DataResourceRecordUtil.RELATED_NEW_VERSION_OF)) {
        String oldUrl = item.getValue();
        item.setValue(urlToPredecessor);
        item.setIdentifierType(Identifier.IDENTIFIER_TYPE.URL);
        LOG.trace("Fix related identifier 'isDerivedFrom' : '{}' -> '{}'", oldUrl, urlToPredecessor);
        foundOldIdentifier = true;
      }
    }
    if (!foundOldIdentifier) {
      RelatedIdentifier newRelatedIdentifier = RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_NEW_VERSION_OF, urlToPredecessor, null, null);
      newRelatedIdentifier.setIdentifierType(Identifier.IDENTIFIER_TYPE.URL);
      newDataResource.getRelatedIdentifiers().add(newRelatedIdentifier);
    }
  }

  /**
   * Delete a digital object with given identifier.
   *
   * @param applicationProperties Configuration properties.
   * @param id                    Identifier of digital object.
   * @param eTag                  ETag of the old digital object.
   * @param supplier              Function for updating record.
   */
  public static void deleteDataResourceRecord(MetastoreConfiguration applicationProperties,
                                              String id,
                                              String eTag,
                                              UnaryOperator<String> supplier) {
    // Check if resource has dependencies (e.g., metadata records depending on schema which are not deleted yet)
    Specification<DataResource> metadataDocumentWithGivenSchema = findBySchemaId(null, List.of(id));
    metadataDocumentWithGivenSchema = findByStateOnly(metadataDocumentWithGivenSchema, DataResource.State.FIXED, DataResource.State.VOLATILE, DataResource.State.REVOKED);
    Pageable pageable = PageRequest.of(0, 1);
    Page<DataResource> dataResources = queryDataResources(metadataDocumentWithGivenSchema, pageable);
    if (dataResources.hasContent()) {
      String message = "Cannot delete resource with id '" + id + "' due to at least one existing dependency! (" + dataResources.get().findFirst().get().getId() + ")";
      LOG.error(message);
      throw new ResourceAlreadyExistException(message);
    }
    DataResourceUtils.deleteResource(applicationProperties, id, eTag, supplier);
    try {
      DataResourceUtils.getResourceByIdentifierOrRedirect(applicationProperties, id, null, supplier);
    } catch (ResourceNotFoundException rnfe) {
    }
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
   * Validate metadata document with given schema.
   *
   * @param metastoreProperties Configuration for accessing services
   * @param schemaRecord        metadata of the schema document.
   * @param document            document
   */
  private static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
                                               MultipartFile document,
                                               SchemaUrl2Path schemaRecord) {
    LOG.trace("validateMetadataDocument (schemaRecord) {},{}, {}", metastoreProperties, schemaRecord, document);
    if (document == null || document.isEmpty()) {
      String message = "Missing metadata document in body. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    try {
      SchemaRecordUtil.validateMetadataDocument(metastoreProperties, document.getInputStream(), schemaRecord);
    } catch (IOException ex) {
      LOG.error("Error while validating metadata document!", ex);
      throw new CustomInternalServerError("Error while validating against schema '" + schemaRecord.getUrl() + "'!");
    }
  }

  public static DataResource getRecordById(MetastoreConfiguration metastoreProperties,
                                           String recordId) throws ResourceNotFoundException {
    return getRecordByIdAndVersion(metastoreProperties, recordId, null);
  }

  public static DataResource getMetadataRecordByIdAndVersion(MetastoreConfiguration metastoreProperties,
                                                             String recordId, String version) throws ResourceNotFoundException {
    DataResource returnValue = getRecordByIdAndVersion(metastoreProperties, recordId, version);
    if (!returnValue.getResourceType().getValue().endsWith(METADATA_SUFFIX)) {
      throw new ResourceNotFoundException("Metadata document with ID '" + recordId + "' doesn't exist!");
    }
    return returnValue;
  }
  /**
   * Get a schema record by given id and version. If version is null, the latest version will be returned.
   * @param metastoreProperties Configuration for accessing services
   * @param recordId Id of the record to be obtained.
   * @param version Version of the record to be obtained. If null, the latest version will be returned.
   * @return DataResource with given id and version.
   * @throws ResourceNotFoundException If no record with given id and version exists or if the record is not a schema record.
   */
  public static DataResource getSchemaRecordByIdAndVersion(MetastoreConfiguration metastoreProperties,
                                                           String recordId, String version) throws ResourceNotFoundException {
    DataResource returnValue = getRecordByIdAndVersion(metastoreProperties, recordId, version);
    if (!returnValue.getResourceType().getValue().endsWith(SCHEMA_SUFFIX)) {
      throw new ResourceNotFoundException("Schema document with ID '" + recordId + "' doesn't exist!");
    }
    return returnValue;
  }

  /**
   * Get a record by given id and version. If version is null, the latest version will be returned.
   * @param metastoreProperties Configuration for accessing services
   * @param recordId Id of the record to be obtained.
   * @param version Version of the record to be obtained. If null, the latest version will be returned.
   * @return DataResource with given id and version.
   * @throws ResourceNotFoundException If no record with given id and version exists.
   */
  public static DataResource getRecordByIdAndVersion(MetastoreConfiguration metastoreProperties,
                                                     String recordId, String version) throws ResourceNotFoundException {
    LOG.trace("Obtaining record with id {} and version {}.", recordId, version);
    //if security enabled, check permission -> if not matching, return HTTP UNAUTHORIZED or FORBIDDEN
    long nano = System.nanoTime() / 1000000;
    long nano2;
    Page<DataResource> dataResource;
    try {
      dataResource = metastoreProperties.getDataResourceService().findAllVersions(recordId, null);
      if (LOG.isTraceEnabled()) {
        for (DataResource item : dataResource.getContent()) {
          LOG.trace("Id: '{}' - Version: '{}' -> Type: '{}'", item.getId(), item.getVersion(), item.getResourceType().toString());
        }
      }
    } catch (ResourceNotFoundException ex) {
      ex.setDetail("Document with ID '" + recordId + "' doesn't exist!");
      throw ex;
    }
    nano2 = System.nanoTime() / 1000000;
    Stream<DataResource> stream = dataResource.get();
    if (version != null && !version.isEmpty()) {
      stream = stream.filter(resource -> resource.getVersion().equals(version));
    }
    Optional<DataResource> findFirst = stream.findFirst();
    if (findFirst.isEmpty()) {
      String message = String.format("Version '%s' of ID '%s' doesn't exist!", version, recordId);
      LOG.error(message);
      throw new ResourceNotFoundException(message);
    }
    long nano3 = System.nanoTime() / 1000000;
    LOG.info("getRecordByIdAndVersion {}, {}, {}", nano, (nano2 - nano), (nano3 - nano));
    return findFirst.get();
  }

  public static ContentInformation getContentInformationByIdAndVersion(MetastoreConfiguration metastoreProperties,
                                                                       String recordId, String version) throws ResourceNotFoundException {
    LOG.trace("Obtaining content information record with id {} and version {}.", recordId, version);
    ContentInformation returnValue = null;
    Optional<Resource2FileVersion> find;
    if (version == null) {
      find = resource2FileVersionDao.findFirstByResourceIdOrderByVersionDesc(recordId);
    } else {
      find = resource2FileVersionDao.findByResourceIdAndVersion(recordId, version);
    }

    if (find.isEmpty()) {
      String message = String.format("No content information found for resource id '%s' and version '%s'!", recordId, version);
      for (Resource2FileVersion item : resource2FileVersionDao.findAll()) {
        LOG.trace(item.toString());
      }
      LOG.error(message);
      throw new ResourceNotFoundException(message);
    }
    return metastoreProperties.getContentInformationService().getContentInformation(recordId, null, find.get().getFileVersion().longValue());
  }

  public static Path getMetadataDocumentByIdAndVersion(MetastoreConfiguration metastoreProperties,
                                                       String recordId, String version) throws ResourceNotFoundException {
    LOG.trace("Obtaining content information record with id {} and version {}.", recordId, version);
    ContentInformation contentRecord = getContentInformationByIdAndVersion(metastoreProperties, recordId, version);

    Path metadataDocumentPath = testForRegularFile(contentRecord.getContentUri());

    return metadataDocumentPath;
  }

  /**
   * Add specification to find data resource by access rights. If caller has
   * administration rights all resources will be found.
   *
   * @param specification Specification for DataResource.
   * @return Refined specification for DataResource.
   */
  public static Specification<DataResource> findByAccessRights(Specification<DataResource> specification) {
    specification = initializeSpecification(specification);
    // Add authentication if enabled
    if (schemaConfig.isAuthEnabled()) {
      boolean isAdmin;
      isAdmin = AuthenticationHelper.hasAuthority(RepoUserRole.ADMINISTRATOR.toString());
      // Add authorization for non administrators
      if (!isAdmin) {
        List<String> authorizationIdentities = AuthenticationHelper.getAuthorizationIdentities();
        if (authorizationIdentities != null) {
          LOG.trace("Creating (READ) permission specification. '{}'", authorizationIdentities);
          Specification<DataResource> permissionSpec = PermissionSpecification.toSpecification(authorizationIdentities, PERMISSION.READ);
          specification = specification.and(permissionSpec);
        } else {
          LOG.trace("No permission information provided. Skip creating permission specification.");
        }
      }
    }
    return specification;
  }

  /**
   * Add specification to find data resource by states. If caller has
   * administration rights all resources will be found.
   *
   * @param specification Specification for DataResource.
   * @param states        Specifiy allowed states.
   * @return Refined specification for DataResource.
   */
  public static Specification<DataResource> findByStateWithAuthorization(Specification<DataResource> specification, DataResource.State... states) {
    specification = initializeSpecification(specification);
    // Add authentication if enabled
    if (schemaConfig.isAuthEnabled()) {
      boolean isAdmin;
      isAdmin = AuthenticationHelper.hasAuthority(RepoUserRole.ADMINISTRATOR.toString());
      // Add valid states for non administrators
      if (!isAdmin) {
        specification = findByStateOnly(specification, states);
      } else {
        LOG.trace("Administrator will find all resources regardless the state.");
      }
    }
    return specification;
  }

  /**
   * Add specification to find data resource by states regardless of users
   * rights.
   *
   * @param specification Specification for DataResource.
   * @param states        Specifiy allowed states.
   * @return Refined specification for DataResource.
   */
  public static Specification<DataResource> findByStateOnly(Specification<DataResource> specification, DataResource.State... states) {
    specification = initializeSpecification(specification);

    List<DataResource.State> stateList = Arrays.asList(states);
    specification = specification.and(StateSpecification.toSpecification(stateList));

    return specification;
  }

  /**
   * Create specification for all listed schemaIds.
   *
   * @param specification Specification for search.
   * @param schemaIds     Provided schemaIDs...
   * @return Specification with schemaIds added.
   */
  public static Specification<DataResource> findBySchemaId(Specification<DataResource> specification, List<String> schemaIds) {
    Specification<DataResource> specWithSchema = initializeSpecification(specification);
    if (schemaIds != null) {
      List<String> allSchemaIds = new ArrayList<>();
      for (String schemaId : schemaIds) {
        allSchemaIds.add(schemaId);
        List<SchemaUrl2Path> allVersions = schemaUrl2PathDao.findBySchemaIdOrderByVersionDesc(schemaId);
        for (SchemaUrl2Path schemaRecord : allVersions) {
          allSchemaIds.add(schemaRecord.getUrl());
        }
      }
      if (!allSchemaIds.isEmpty()) {
        specWithSchema = specWithSchema.and(RelatedIdentifierSpec.toSpecification(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, allSchemaIds.toArray(String[]::new)));
      }
    }
    return specWithSchema;
  }

  /**
   * Add specification to find data resource of schema documents by mimetype.
   *
   * @param specification Specification for DataResource.
   * @param mimeTypes     Provided mimetypes.
   * @return Refined specification for DataResource.
   */
  public static final Specification<DataResource> findByMimetypes(Specification<DataResource> specification, List<String> mimeTypes) {
    specification = initializeSpecification(specification);
    // Search for both mimetypes (xml & json)
    ResourceType resourceType;
    final int JSON = 1; // bit 0
    final int XML = 2;  // bit 1
    // 
    int searchFor = 0; // 1 - JSON, 2 - XML, 3 - both
    if (mimeTypes != null) {
      for (String mimeType : mimeTypes) {
        if (mimeType.contains("json")) {
          searchFor |= JSON;
        }
        if (mimeType.contains("xml")) {
          searchFor |= XML;
        }
      }
    } else {
      searchFor = JSON | XML;
    }
    resourceType = switch (searchFor) {
      // 1 -> search for JSON only
      case JSON ->
              ResourceType.createResourceType(DataResourceRecordUtil.JSON_SCHEMA_TYPE, ResourceType.TYPE_GENERAL.MODEL);
      // 2 -> search for XML only
      case XML ->
              ResourceType.createResourceType(DataResourceRecordUtil.XML_SCHEMA_TYPE, ResourceType.TYPE_GENERAL.MODEL);
      // 3 -> Search for both mimetypes (xml & json)
      case JSON | XML ->
              ResourceType.createResourceType(DataResourceRecordUtil.SCHEMA_SUFFIX, ResourceType.TYPE_GENERAL.MODEL);
      // 0 -> Unknown mimetype
      default -> ResourceType.createResourceType("unknown");
    };

    return specification.and(ResourceTypeSpec.toSpecification(resourceType));
  }

  /**
   * Create specification for all listed related data resources
   * (IS_METADATA_FOR).
   *
   * @param specification Specification for search.
   * @param relatedIds    Provided schemaIDs...
   * @return Specification with related data resources added.
   */
  public static Specification<DataResource> findByRelatedId(Specification<DataResource> specification, List<String> relatedIds) {
    specification = initializeSpecification(specification);
    if ((relatedIds != null) && !relatedIds.isEmpty()) {
      specification = specification.and(RelatedIdentifierSpec.toSpecification(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, relatedIds.toArray(String[]::new)));
    }
    return specification;
  }

  /**
   * Create specification for all listed related data resources
   * (IS_METADATA_FOR).
   *
   * @param specification Specification for search.
   * @param updateFrom    Start date of date range.
   * @param updateUntil   End date of date range.
   * @return Specification with date range added.
   */
  public static Specification<DataResource> findByUpdateDates(Specification<DataResource> specification, Instant updateFrom, Instant updateUntil) {
    specification = initializeSpecification(specification);
    if ((updateFrom != null) || (updateUntil != null)) {
      specification = specification.and(LastUpdateSpecification.toSpecification(updateFrom, updateUntil));
    }
    return specification;
  }

  /**
   * Find by resource type. Only 2 resource types are valid:
   * <ul> <li> Schema documents </li>
   * <li> Metadata documents </li> </ul>
   *
   * @param specification Specification for search.
   * @param resourceType  Specification with resource type added.
   * @return
   */
  public static Specification<DataResource> findByResourceType(Specification<DataResource> specification, String resourceType) {
    specification = initializeSpecification(specification);
    // Search for resource type either of schema or metadata
    Specification<DataResource> resourceTypeSpec = ResourceTypeSpec.toSpecification(ResourceType.createResourceType(resourceType, ResourceType.TYPE_GENERAL.MODEL));
    return specification.and(resourceTypeSpec);

  }

  private static Specification<DataResource> initializeSpecification(Specification<DataResource> specification) {
    if (specification == null) {
      specification = Specification.where(null);
    }
    return specification;
  }

  /**
   * Check validity of acl list and then merge new acl list in the existing one.
   *
   * @param managed  Existing metadata record.
   * @param provided New metadata record.
   * @return Merged list
   */
  public static Set<AclEntry> mergeAcl(Set<AclEntry> managed, Set<AclEntry> provided) {
    // Check for null parameters (which shouldn't happen)
    managed = (managed == null) ? new HashSet<>() : managed;
    provided = (provided == null) ? new HashSet<>() : provided;
    if (!provided.isEmpty()) {
      // check for equality of both lists ignoring ids.
      if (!checkForEquality(managed, provided)) {
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
   * @param managed     Existing value.
   * @param provided    New value.
   * @return Merged record
   */
  public static <T> T mergeEntry(String description, T managed, T provided) {
    return mergeEntry(description, managed, provided, false);
  }

  /**
   * Set new value for existing one.
   *
   * @param description       For logging purposes only
   * @param managed           Existing value.
   * @param provided          New value.
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
   * same document this will be counted as one.
   *
   * @return Number of registered documents.
   */
  public static long getNoOfMetadataDocuments() {
    // Search for resource type of MetadataSchemaRecord
    Specification<DataResource> spec = DataResourceRecordUtil.findByResourceType(null, METADATA_SUFFIX);
    Pageable pgbl = PageRequest.of(0, 1);
    return queryDataResources(spec, pgbl).getTotalElements();
  }

  /**
   * Return the number of ingested schema documents. If there are two versions
   * of the same document this will be counted as one.
   *
   * @return Number of registered documents.
   */
  public static long getNoOfSchemaDocuments() {
    // Search for resource type of MetadataSchemaRecord
    Specification<DataResource> spec = DataResourceRecordUtil.findByResourceType(null, SCHEMA_SUFFIX);
    Pageable pgbl = PageRequest.of(0, 1);
    return queryDataResources(spec, pgbl).getTotalElements();
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
   * @param aDataResourceDao the dataResourceDao to set
   */
  public static void setDataResourceDao(IDataResourceDao aDataResourceDao) {
    dataResourceDao = aDataResourceDao;
  }

  /**
   * Set the DAO holding url and paths.
   *
   * @param aSchemaUrl2PathDao the schemaUrl2PathDao to set
   */
  public static void setSchemaUrl2PathDao(ISchemaUrl2PathDao aSchemaUrl2PathDao) {
    schemaUrl2PathDao = aSchemaUrl2PathDao;
  }

  /**
   * @param allIdentifiersDao the allIdentifiersDao to set
   */
  public static void setAllIdentifiersDao(IAllIdentifiersDao allIdentifiersDao) {
    DataResourceRecordUtil.allIdentifiersDao = allIdentifiersDao;
  }

  /**
   * Set the DAO holding identifiers and their file versions.
   *
   * @param aResource2FileVersionDao the identifier2FileVersionDao to set
   */
  public static void setResource2FileVersionDao(IResource2FileVersionDao aResource2FileVersionDao) {
    DataResourceRecordUtil.resource2FileVersionDao = aResource2FileVersionDao;
  }

  public static final void fixSchemaUrl(DataResource dataresource) {
    RelatedIdentifier schemaIdentifier = getSchemaIdentifier(dataresource);
    fixSchemaUrl(schemaIdentifier);
  }

  public static final void fixSchemaUrl(RelatedIdentifier schemaIdentifier) {
    if (schemaIdentifier != null && schemaIdentifier.getIdentifierType().equals(Identifier.IDENTIFIER_TYPE.INTERNAL)) {
      String value = schemaIdentifier.getValue();
      StringTokenizer tokenizer = new StringTokenizer(schemaIdentifier.getValue(), SCHEMA_VERSION_SEPARATOR);
      String version = null;
      String schemaId = null;
      SchemaUrl2Path schemaRecord = null;
      switch (tokenizer.countTokens()) {
        case 2:
          schemaId = tokenizer.nextToken();
          version = tokenizer.nextToken();
          schemaRecord = schemaUrl2PathDao.findBySchemaIdAndVersion(schemaId, version).orElse(null);
          break;
        case 1:
          schemaId = tokenizer.nextToken();
          schemaRecord = schemaUrl2PathDao.findFirstBySchemaIdOrderByVersionDesc(schemaId).orElse(null);
          break;
        default:
          throw new CustomInternalServerError("Invalid schemaId!");
      }
      if (schemaRecord == null) {
        for (SchemaUrl2Path item : schemaUrl2PathDao.findAll()) {
          LOG.trace("Existing schema record: {}", item);
        }
        LOG.trace("Number of schema records: {}", schemaUrl2PathDao.count());
        throw new CustomInternalServerError("No schema record found for schemaId '" + value + "'!");
      }

      schemaIdentifier.setValue(schemaRecord.getUrl());
      schemaIdentifier.setIdentifierType(Identifier.IDENTIFIER_TYPE.URL);
      LOG.trace("Fix scheme Url '{}' -> '{}'", value, schemaIdentifier.getValue());
    }
  }

  /**
   * Get all versions for a given data resource ID.
   *
   * @param dataResourceId Data resource ID.
   * @return List of all versions (may be empty).
   */
  public static List<DataResource> getAllVersions(String dataResourceId, Pageable pgbl) {
    List<DataResource> allVersions = schemaConfig.getDataResourceService().findAllVersions(dataResourceId, pgbl).getContent();
    String lastVersion = "0.0.0";
    List<DataResource> filteredList = new ArrayList<>();
    for (DataResource dataResource : allVersions) {
      if (!lastVersion.equals(dataResource.getVersion())) {
        lastVersion = dataResource.getVersion();
        filteredList.add(dataResource);
      }
    }
    return filteredList;
  }

  /**
   * Check and update license information of data resource.
   *
   * @param dataResource Data resource to be checked.
   * @param licenseUri   New license URI.
   */
  public static void checkLicense(DataResource dataResource, String licenseUri) {
    if (licenseUri != null) {
      Set<Scheme> rights = dataResource.getRights();
      String licenseId = licenseUri.substring(licenseUri.lastIndexOf(SCHEMA_VERSION_SEPARATOR));
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

  /**
   * Test if exactly one schema and one related resource exists. This method
   * does NOT check the correctness of the references.
   *
   * @param dataResource Data resource of a metadata document.
   * @throws BadArgumentException Related resources are not defined as expected.
   */
  public static void validateRelatedResources4MetadataDocuments(DataResource dataResource) throws BadArgumentException {
    int noOfRelatedData = 0;
    int noOfRelatedSchemas = 0;
    if (dataResource != null) {
      Set<RelatedIdentifier> relatedResources = dataResource.getRelatedIdentifiers();

      // Check if related resource already exists (only one related resource of type hasMetadata is allowed)
      for (RelatedIdentifier item : relatedResources) {
        if (item.getRelationType() == DataResourceRecordUtil.RELATED_SCHEMA_TYPE) {
          noOfRelatedSchemas++;
        }
        if (item.getRelationType() == DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE) {
          noOfRelatedData++;
        }
      }
    }
    checkNoOfRelatedIdentifiers(noOfRelatedData, noOfRelatedSchemas);
  }

  /**
   * Validate related identifiers. There has to be exactly one schema
   * (hasMetadata) and at *least* one related data resource.
   *
   * @param noOfRelatedData    No of related data resources.
   * @param noOfRelatedSchemas No of related schemas.
   */
  private static void checkNoOfRelatedIdentifiers(int noOfRelatedData, int noOfRelatedSchemas) {
    if ((noOfRelatedSchemas != 1) || (noOfRelatedData == 0)) {
      String errorMessage = "";
      if (noOfRelatedSchemas == 0) {
        errorMessage = "Mandatory attribute relatedIdentifier of type '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "' was not found in record. \n";
      }
      if (noOfRelatedSchemas > 1) {
        errorMessage = "Mandatory attribute relatedIdentifier of type '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "' was provided more than once in record. \n";
      }
      if (noOfRelatedData == 0) {
        errorMessage = errorMessage + "Mandatory attribute relatedIdentifier of type '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "' was not found in record. \n";
      }
      errorMessage = errorMessage + "Returning HTTP BAD_REQUEST.";
      LOG.error(errorMessage);
      throw new BadArgumentException(errorMessage);
    }
  }

  /**
   * Get schema identifier of data resource.
   *
   * @param dataResourceRecord Metadata record hold schema identifier.
   * @return RelatedIdentifier with a global accessible identifier.
   */
  public static RelatedIdentifier getSchemaIdentifier(DataResource dataResourceRecord) {
    LOG.trace("Get schema identifier for '{}'.", dataResourceRecord.getId());
    return getRelatedIdentifier(dataResourceRecord, DataResourceRecordUtil.RELATED_SCHEMA_TYPE);
  }

  /**
   * Transform schema identifier to global available identifier (if neccessary).
   *
   * @param dataResourceRecord Metadata record hold schema identifier.
   * @param relationType       Relation type of the identifier.
   * @return ResourceIdentifier with a global accessible identifier.
   */
  public static RelatedIdentifier getRelatedIdentifier(DataResource dataResourceRecord, RelatedIdentifier.RELATION_TYPES relationType) {
    LOG.trace("Get related identifier for '{}' of type '{}'.", dataResourceRecord.getId(), relationType);
    RelatedIdentifier relatedIdentifier = null;

    Set<RelatedIdentifier> relatedResources = dataResourceRecord.getRelatedIdentifiers();

    // Check if related resource already exists (only one related resource of type DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE allowed)
    for (RelatedIdentifier item : relatedResources) {
      if (item.getRelationType().equals(relationType)) {
        relatedIdentifier = item;
      }
    }
    return relatedIdentifier;
  }

  /**
   * Get description of data resource with given type.
   *
   * @param dataResourceRecord Data resource record holding related identifier(s).
   * @param descriptionType    Description type of the identifier.
   * @return ResourceIdentifier with a global accessible identifier.
   */
  public static Description getDescription(DataResource dataResourceRecord, Description.TYPE descriptionType) {
    LOG.trace("Get description for '{}' of type '{}'.", dataResourceRecord.getId(), descriptionType);
    Description description = null;

    Set<Description> descriptions = dataResourceRecord.getDescriptions();

    // Check if description already exists
    for (Description item : descriptions) {
      if (item.getType().equals(descriptionType)) {
        description = item;
      }
    }
    return description;
  }

  /**
   * Check if ID for schema is valid. Requirements: - shouldn't change if URL
   * encoded - should be lower case If it's not lower case the original ID will
   * we set as an alternate ID.
   *
   * @param metadataRecord Datacite Record.
   */
  public static final void check4validSchemaId(DataResource metadataRecord) {
    // schema id should be lower case due to elasticsearch
    // alternate identifier is used to set id to a given id.
    check4validId(metadataRecord, false);
  }

  public static final void check4validId(DataResource metadataRecord, boolean allowUpperCase) {
    String id = metadataRecord.getId();
    String lowerCaseId;
    lowerCaseId = id.toLowerCase(Locale.getDefault());

    if (allowUpperCase) {
      lowerCaseId = id;
    }
    metadataRecord.getAlternateIdentifiers().add(Identifier.factoryInternalIdentifier(lowerCaseId));
    if (!lowerCaseId.equals(id)) {
      metadataRecord.getAlternateIdentifiers().add(Identifier.factoryIdentifier(id, Identifier.IDENTIFIER_TYPE.OTHER));
    }

    String value = URLEncoder.encode(metadataRecord.getId(), StandardCharsets.UTF_8);
    if (!value.equals(metadataRecord.getId())) {
      String message = "Not a valid ID! Encoded: " + value;
      LOG.error(message);
      throw new BadArgumentException(message);
    }

  }

  private static void validateMetadataSchemaDocument(MetastoreConfiguration metastoreProperties, DataResource dataResourceRecord, MultipartFile document) {
    LOG.debug("Validate metadata schema document...");
    if (document == null || document.isEmpty()) {
      String message = "Missing metadata schema document in body. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    try {
      validateMetadataSchemaDocument(metastoreProperties, dataResourceRecord, document.getBytes());
    } catch (IOException ex) {
      String message = LOG_ERROR_READ_METADATA_DOCUMENT;
      LOG.error(message, ex);
      throw new UnprocessableEntityException(message);
    }
  }

  private static void validateMetadataSchemaDocument(MetastoreConfiguration metastoreProperties, DataResource dataResource, byte[] document) {
    LOG.debug("Validate metadata schema document...");
    if (document == null || document.length == 0) {
      String message = "Missing metadata schema document in body. Returning HTTP BAD_REQUEST.";
      LOG.error(message);
      throw new BadArgumentException(message);
    }

    IValidator applicableValidator;
    try {
      applicableValidator = getValidatorForRecord(metastoreProperties, dataResource, document);

      if (applicableValidator == null) {
        String message = "No validator found for schema type " + dataResource.getResourceType().getValue() + ". Returning HTTP UNPROCESSABLE_ENTITY.";
        LOG.error(message);
        throw new UnprocessableEntityException(message);
      } else {
        LOG.trace("Validator found. Checking provided schema file.");
        LOG.trace("Performing validation of metadata document using schema {}, version {} and validator {}.", dataResource.getId(), dataResource.getVersion(), applicableValidator);
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

  /**
   * Fix resource type and format of data resource if not provided. This is necessary for validation and indexing.
   *
   * @param dataResource        Data resource to be checked.
   * @param mimeType Detected mimetype of the schema document.
   */
  private static void fixResourceTypeAndFormat(DataResource dataResource, String mimeType) {
    if (mimeType != null) {
      dataResource.getFormats().add(mimeType);
    }
    if ((dataResource.getResourceType() == null)
            || (dataResource.getResourceType().getValue() == null)) {

      if (mimeType == null) {
        String message = "Unable to detect schema type automatically. Please provide a valid type";
        LOG.error(message);
        throw new UnprocessableEntityException(message);
      } else {
        String type;
        if (mimeType.contains("json")) {
          type = JSON_SCHEMA_TYPE;
        } else {
          type = XML_SCHEMA_TYPE;
        }
        dataResource.setResourceType(ResourceType.createResourceType(type, ResourceType.TYPE_GENERAL.MODEL));
        LOG.debug("Automatically detected mimetype of schema: '{}' -> '{}'.", mimeType, type);
      }
    }
    // Also fix format if necessary and possible
    String type = dataResource.getResourceType().getValue();
    if (dataResource.getFormats().isEmpty()) {
      if (type.toLowerCase().contains("json") ) {
        dataResource.getFormats().add(MediaType.APPLICATION_JSON_VALUE);
      } else {
        if (type.toLowerCase().contains("xml")) {
          dataResource.getFormats().add(MediaType.APPLICATION_XML_VALUE);
        }
      }
    } else {
      LOG.trace("Provided format(s) for schema document: '{}'", dataResource.getFormats());
      if (mimeType.toLowerCase().contains("json") && !dataResource.getResourceType().getValue().toLowerCase().contains("json") ||
              mimeType.toLowerCase().contains("xml") && !dataResource.getResourceType().getValue().toLowerCase().contains("xml")) {
        // resource type doesn't match provided format(s) -> throw BadArgumentException
        String message = "Provided resource type '" + dataResource.getResourceType().getValue() + "' doesn't match provided format(s) '" + dataResource.getFormats() + "'. Returning HTTP BAD_REQUEST.";
        LOG.error(message);
        throw new BadArgumentException(message);
      }
    }
  }

  private static IValidator getValidatorForRecord(MetastoreConfiguration metastoreProperties, DataResource dataResourceRecord, byte[] schemaDocument) {
    IValidator applicableValidator = null;
    String mimeType = null;
    Set<String> formatsSet = dataResourceRecord.getFormats();
    List<String> formats = new ArrayList<String>();
    formats.addAll(formatsSet);
    formats.add(null);

    for (String format : formats) {
      LOG.trace("Provided format for schema document: '{}'", format);
      applicableValidator = SchemaRecordUtil.getValidatorForRecord(metastoreProperties, format, schemaDocument);
      if (applicableValidator != null) {
        mimeType = format != null ? format : SchemaUtils.guessMimetype(schemaDocument);
        break;
      }
    }
    fixResourceTypeAndFormat(dataResourceRecord, mimeType);
    return applicableValidator;
  }

  private static DataResource checkParameters(MultipartFile dataResourceRecord, MultipartFile document, boolean bothRequired) {
    boolean recordNotAvailable;
    boolean documentNotAvailable;
    DataResource metadataRecord = null;

    recordNotAvailable = dataResourceRecord == null || dataResourceRecord.isEmpty();
    documentNotAvailable = document == null || document.isEmpty();
    String message = null;
    if (bothRequired && (recordNotAvailable || documentNotAvailable)) {
      message = "No data resource record and/or metadata document provided. Returning HTTP BAD_REQUEST.";
    } else {
      if (!bothRequired && recordNotAvailable && documentNotAvailable) {
        message = "Neither metadata record nor metadata document provided.";
      }
    }
    if (message != null) {
      LOG.error(message);
      throw new BadArgumentException(message);
    }
    // Do some checks first.
    if (!recordNotAvailable) {
      metadataRecord = getDataResourceFromBody(dataResourceRecord);
    }
    return metadataRecord;
  }

  private static DataResource getDataResourceFromBody(MultipartFile dataResourceRecord) {
    DataResource metadataRecord = null;
    try {
      metadataRecord = Json.mapper().readValue(dataResourceRecord.getInputStream(), DataResource.class);
    } catch (IOException ex) {
      String message = "Can't map record document to DataResource";
      if (ex instanceof JsonParseException) {
        message = message + " Reason: " + ex.getMessage();
      }
      LOG.error(ERROR_PARSING_JSON, ex);
      throw new BadArgumentException(message);
    }
    return metadataRecord;
  }

  /**
   * Validate metadata document with given schema. In case of an error a runtime
   * exception is thrown.
   *
   * @param metastoreProperties Configuration properties.
   * @param document            Document to validate.
   * @param schemaId            SchemaId of schema.
   * @param version             Version of the document.
   */
  public static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
                                              MultipartFile document,
                                              String schemaId,
                                              String version) {
    LOG.trace("validateMetadataDocument (schemaId) {},SchemaID {}, Version {}, {}", metastoreProperties, schemaId, version, document);
    SchemaUrl2Path schemaRecord;
    DataResource dataResource = DataResourceRecordUtil.getRecordById(metastoreProperties, schemaId);
    if (dataResource == null) {
      String message = "Unknown schemaID '" + schemaId + "'!";
      LOG.error(message);
      throw new ResourceNotFoundException(message);
    }
    schemaId = dataResource.getId();
    if (version != null) {
      if (SemanticVersion.tryParse(version).isEmpty()) {
        throw new BadArgumentException("Invalid SemVer format (MAJOR.MINOR.PATCH only, without any extras). Provided Version: '" + version + "'");
      }
      schemaRecord = schemaUrl2PathDao.findBySchemaIdAndVersion(schemaId, version).orElse(null);
      if (schemaRecord == null) {
        String message = "Unknown schemaID '" + schemaId + "' and version '" + version + "'!";
        LOG.error(message);
        throw new ResourceNotFoundException(message);
      }
    } else {
      schemaRecord = schemaUrl2PathDao.findFirstBySchemaIdOrderByVersionDesc(schemaId).orElse(null);
    }
    validateMetadataDocument(metastoreProperties, document, schemaRecord);
  }

  private static SchemaUrl2Path getSchemaRecordFromDataResource(DataResource dataResource) {
    SchemaUrl2Path schemaRecord = null;
    RelatedIdentifier schemaIdentifier = getSchemaIdentifier(dataResource);
    if ((schemaIdentifier != null) && (schemaIdentifier.getValue() != null)) {
      String schemaId = schemaIdentifier.getValue();
      LOG.trace("getSchemaRecordFromDataResource: related identifier:  '{}'", schemaIdentifier);
      LOG.trace("getSchemaRecordFromDataResource: '{}'", schemaId);
      switch (schemaIdentifier.getIdentifierType()) {
        case URL:
          schemaRecord = schemaUrl2PathDao.findByUrl(schemaIdentifier.getValue()).orElse(null);
          break;
        case INTERNAL:
          schemaRecord = schemaUrl2PathDao.findFirstBySchemaIdOrderByVersionDesc(schemaId).orElse(null);
          break;
        default:
          String message = "Unsupported identifier type: '" + schemaIdentifier.getIdentifierType() + "'!";
          LOG.error(message);
          throw new ResourceNotFoundException(message);
      }
    }
    return schemaRecord;
  }

  /**
   * Update schema document.
   *
   * @param applicationProperties Settings of repository.
   * @param resourceId            ID of the schema document.
   * @param eTag                  E-Tag of the current schema document.
   * @param recordDocument        Record of the schema.
   * @param schemaDocument        Schema document.
   * @param supplier              Method for creating access URL.
   * @return Record of updated schema document.
   */
  public static DataResource updateDataResource4SchemaDocument(MetastoreConfiguration applicationProperties,
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
    if (metadataRecord != null) {
      metadataRecord.setId(dataResource.getId());
      // It's not possible to change the resource type of a record. So we have to set it to the old one.
      metadataRecord.setResourceType(dataResource.getResourceType());
      // Also formats have to be set to the old one, otherwise they would be lost if not provided in the new record.
      metadataRecord.setFormats(dataResource.getFormats());

      dataResource = metadataRecord;
    } else {
      dataResource = DataResourceUtils.copyDataResource(dataResource);
    }
    return updateDataResource4SchemaDocument(applicationProperties, resourceId, eTag, dataResource, schemaDocument, supplier);
  }

  /**
   * Update schema document.
   *
   * @param applicationProperties Settings of repository.
   * @param resourceId            ID of the schema document.
   * @param eTag                  E-Tag of the current schema document.
   * @param givenDataResource     Record of the schema.
   * @param schemaDocument        Schema document.
   * @param supplier              Method for creating access URL.
   * @return Record of updated schema document.
   */
  public static DataResource updateDataResource4SchemaDocument(MetastoreConfiguration applicationProperties,
                                                               String resourceId,
                                                               String eTag,
                                                               DataResource givenDataResource,
                                                               MultipartFile schemaDocument,
                                                               UnaryOperator<String> supplier) {
    DataResource updatedDataResource;
    LOG.trace("Obtaining most recent datacite record with id {}.", resourceId);
    DataResource oldDataResource = applicationProperties.getDataResourceService().findById(resourceId);
    LOG.trace("Checking provided ETag.");
    ControllerUtils.checkEtag(eTag, oldDataResource);
    LOG.trace("ETag: '{}'", oldDataResource.getEtag());
    updatedDataResource = mergeDataResource(oldDataResource, givenDataResource);
    if (schemaDocument != null) {
      updateSchemaDocument(applicationProperties, updatedDataResource, schemaDocument, supplier);
    } else {
      // Version shouldn't be updated if only metadata is updated, so set version to old one.
      updatedDataResource.setVersion(oldDataResource.getVersion());
      updateOnlyMetadata4SchemaDocument(applicationProperties, updatedDataResource);
    }
    updatedDataResource = DataResourceUtils.updateResource(applicationProperties, updatedDataResource.getId(), updatedDataResource, eTag, supplier);

    return updatedDataResource;
  }

  /**
   * Update metadata document.
   *
   * @param applicationProperties Settings of repository.
   * @param updatedDataResource   DataResource of the metadata/schema document.
   * @param document              Metadata document.
   * @param supplier              Method for creating access URL.
   * @return If there are changes: true, otherwise false.
   */
  private static boolean updateDocument(MetastoreConfiguration applicationProperties,
                                        DataResource updatedDataResource,
                                        MultipartFile document,
                                        UnaryOperator<String> supplier) {
    ContentInformation info;
    String fileName;
    info = getContentInformationOfResource(applicationProperties, updatedDataResource);
    fileName = (info != null) ? info.getRelativePath() : document.getOriginalFilename();
    boolean noChanges = checkDocumentForChanges(info, document);

    if (noChanges) {
      LOG.trace("No changes in document -> No update necessary. Reset version to old one.");
      updatedDataResource.setVersion(info.getFileVersion());
    } else {
      // Everything seems to be fine update document and increment version
      LOG.trace("Updating schema/metadata document (and increment version)...");
      updatedDataResource = check4VersionUpdate(updatedDataResource, info.getFileVersion());
      addProvenance(updatedDataResource);
      ContentInformation updatedContentInformation = ContentDataUtils.addFile(applicationProperties, updatedDataResource, document, fileName, null, true, supplier);
      DataResourceRecordUtil.saveNewResource2FileVersion(updatedDataResource, updatedContentInformation);
      // In case of a schema document we also have to add a schema record
      if (updatedDataResource.getResourceType().getValue().contains(DataResourceRecordUtil.SCHEMA_SUFFIX)) {
        SchemaUrl2Path schemaRecord = createSchemaRecord(updatedDataResource, info);
        DataResourceRecordUtil.saveNewSchemaRecord(schemaRecord);
      }
    }
    return noChanges;
  }

  private static void updateMetadataDocument(MetastoreConfiguration applicationProperties,
                                             DataResource updatedDataResource,
                                             MultipartFile document,
                                             UnaryOperator<String> supplier) {
    SchemaUrl2Path schemaRecord = schemaUrl2PathDao.findByUrl(DataResourceRecordUtil.getRelatedIdentifier(updatedDataResource, DataResourceRecordUtil.RELATED_SCHEMA_TYPE).getValue()).orElse(null);
    validateMetadataDocument(applicationProperties, document, schemaRecord);
    // Everything seems to be fine update document and increment version if necessary
    updateDocument(applicationProperties, updatedDataResource, document, supplier);
  }

  private static void updateSchemaDocument(MetastoreConfiguration applicationProperties,
                                           DataResource updatedDataResource,
                                           MultipartFile schemaDocument,
                                           UnaryOperator<String> supplier) {
    validateMetadataSchemaDocument(applicationProperties, updatedDataResource, schemaDocument);
    // Everything seems to be fine update document and increment version if necessary
    updateDocument(applicationProperties, updatedDataResource, schemaDocument, supplier);
  }

  private static void updateOnlyMetadata4SchemaDocument(MetastoreConfiguration applicationProperties,
                                                        DataResource updatedDataResource) {
    ContentInformation info;
    info = getContentInformationOfResource(applicationProperties, updatedDataResource);
    // validate if document is still valid due to changed record settings.
    Objects.requireNonNull(info);

    Path schemaDocumentPath = testForRegularFile(info.getContentUri());

    try {
      byte[] schemaDoc = Files.readAllBytes(schemaDocumentPath);
      validateMetadataSchemaDocument(applicationProperties, updatedDataResource, schemaDoc);
    } catch (IOException ex) {
      LOG.error("Error validating file!", ex);
    }

  }

  private static DataResource mergeDataResource(DataResource oldDataResource, DataResource givenDataResource) {
    DataResource updatedDataResource;

    if (givenDataResource != null) {
      LOG.trace("new DataResource: '{}'", givenDataResource);
      // version handling willl be done separately, so do nothing right now.
      //givenDataResource.setVersion(oldDataResource.getVersion());
      givenDataResource.setId(oldDataResource.getId());
      updatedDataResource = givenDataResource;
      mergeCreators(oldDataResource, updatedDataResource);
      mergePublicationYear(oldDataResource, updatedDataResource);
      mergePublisher(oldDataResource, updatedDataResource);
      mergeAcl(oldDataResource, updatedDataResource);
      fixEmptyRights(updatedDataResource);
      mergeState(oldDataResource, updatedDataResource);
      mergeResourceType(oldDataResource, updatedDataResource);
      mergeCreateDate(oldDataResource, updatedDataResource);
      mergeFormats(oldDataResource, updatedDataResource);
    } else {
      updatedDataResource = DataResourceUtils.copyDataResource(oldDataResource);
    }
    return updatedDataResource;
  }

  private static void mergeFormats(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null) {
      if ((updatedDataResource.getFormats() == null) || updatedDataResource.getFormats().isEmpty()) {
        LOG.trace("Merging formats from former data resource: '{}'", oldDataResource.getFormats());
        updatedDataResource.setFormats(oldDataResource.getFormats());
      } else {
        LOG.trace("Update formats!");
      }
    }
  }

  private static DataResource mergeCreators(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null) {
      if ((updatedDataResource.getCreators() == null) || updatedDataResource.getCreators().isEmpty()) {
        updatedDataResource.setCreators(oldDataResource.getCreators());
      } else {
        LOG.trace("Update creators!");
      }
    }
    return updatedDataResource;
  }

  private static DataResource mergePublicationYear(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null && updatedDataResource.getPublicationYear() == null) {
      updatedDataResource.setPublicationYear(oldDataResource.getPublicationYear());

    }
    return updatedDataResource;
  }

  private static DataResource mergePublisher(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null && updatedDataResource.getPublisher() == null) {
      updatedDataResource.setPublisher(oldDataResource.getPublisher());

    }
    return updatedDataResource;
  }

  private static DataResource mergeAcl(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null) {
      updatedDataResource.setAcls(mergeAcl(oldDataResource.getAcls(), updatedDataResource.getAcls()));
    }
    return updatedDataResource;
  }

  /**
   * Fix rights to an empty array if no rights are provided.
   *
   * @param updatedDataResource data resource to check.
   * @return Fixed data resource.
   */
  private static DataResource fixEmptyRights(DataResource updatedDataResource) {
    if (updatedDataResource != null && updatedDataResource.getRights() == null) {
      updatedDataResource.setRights(new HashSet<>());
    }
    return updatedDataResource;
  }

  private static DataResource mergeState(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null && updatedDataResource.getState() == null) {
      updatedDataResource.setState(oldDataResource.getState());
    }
    return updatedDataResource;
  }

  private static DataResource mergeResourceType(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null && updatedDataResource.getResourceType() == null) {
      updatedDataResource.setResourceType(oldDataResource.getResourceType());
    }
    return updatedDataResource;
  }

  private static DataResource mergeCreateDate(DataResource oldDataResource, DataResource updatedDataResource) {
    if (updatedDataResource != null) {
      // Set create date
      Date createDate = null;
      for (Date date : oldDataResource.getDates()) {
        if (date.getType().equals(Date.DATE_TYPE.CREATED)) {
          createDate = date;
        }
      }
      Date newCreateDate = null;
      for (Date date : updatedDataResource.getDates()) {
        if (date.getType().equals(Date.DATE_TYPE.CREATED)) {
          newCreateDate = date;
        }
      }
      if (newCreateDate != null) {
        updatedDataResource.getDates().remove(newCreateDate);
      }
      updatedDataResource.getDates().add(createDate);
    }
    return updatedDataResource;
  }

  public static boolean checkDocumentForChanges(ContentInformation info, MultipartFile document) {
    boolean noChanges = true;
    if (info != null) {
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
        throw new BadArgumentException("Error reading schema document!");
      }
    } else {
      throw new CustomInternalServerError("No content information provided!");
    }
    return noChanges;
  }
  // Check for changes...

  /**
   * Migrate schema from INTERNAL type to URL type (if necessary)
   *
   * @param dataResource Data resource which should be fixed.
   * @return Fixed data resource.
   */
  private static DataResource fixRelatedSchemaIfNeeded(DataResource dataResource) {
    RelatedIdentifier relatedIdentifier = getSchemaIdentifier(dataResource);
    SchemaUrl2Path schemaRecord = getSchemaRecordFromDataResource(dataResource);
    if (schemaRecord != null) {
      if (relatedIdentifier != null && relatedIdentifier.getIdentifierType() == Identifier.IDENTIFIER_TYPE.INTERNAL) {
        relatedIdentifier.setIdentifierType(Identifier.IDENTIFIER_TYPE.URL);
        // schemaRecord should never be null for internal schema!
        relatedIdentifier.setValue(schemaRecord.getUrl());
      }
    } else {
      String identifier = relatedIdentifier != null ? relatedIdentifier.getValue() : "is not defined and therefor";
      throw new UnprocessableEntityException("Schema '" + identifier + "' is not known!");
    }
    return dataResource;
  }

  /**
   * Validate metadata document with given schema. Determine type if not already
   * given or check type.
   *
   * @param metastoreProperties Configuration for accessing services
   * @param dataResource        Data resource record of the document.
   * @param document            Document of data resource.
   */
  private static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
                                               MultipartFile document,
                                               DataResource dataResource) {
    LOG.trace("validateMetadataDocument (dataresource) {},{}, {}", metastoreProperties, dataResource, document);
    boolean validationSuccess = false;
    String errorMessage = null;
    SchemaUrl2Path schemaUrl2Path;
    schemaUrl2Path = getSchemaRecordFromDataResource(dataResource);
    try {
      validateMetadataDocument(metastoreProperties, document, schemaUrl2Path);
      validationSuccess = true;
      // After successful validation set type for metadata document resource.
      String type = schemaUrl2Path.getMimetype();
      dataResource.setResourceType(ResourceType.createResourceType(type + METADATA_SUFFIX, ResourceType.TYPE_GENERAL.MODEL));
      // Also fix format if necessary
      if (dataResource.getFormats().isEmpty()) {
        dataResource.getFormats().add(type);
      }
      //
    } catch (Exception ex) {
      String message = "Error validating document!";
      LOG.error(message, ex);
      errorMessage = ex.getMessage();
    }
    if (!validationSuccess) {
      LOG.error(errorMessage);
      throw new UnprocessableEntityException(errorMessage);
    }
  }

  /**
   * Create schema record from DataResource and ContentInformation.
   *
   * @param dataResource       Data resource
   * @param contentInformation Content information
   * @return schema record
   */
  public static final SchemaUrl2Path createSchemaRecord(DataResource dataResource, ContentInformation contentInformation) {
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setSchemaId(dataResource.getId());
    String type = dataResource.getResourceType().getValue();
    if (type.equals(JSON + SCHEMA_SUFFIX)) {
      schemaRecord.setMimetype(MediaType.APPLICATION_JSON_VALUE);
    } else {
      if (type.equals(XML + SCHEMA_SUFFIX)) {
        schemaRecord.setMimetype(MediaType.APPLICATION_XML_VALUE);

      } else {
        throw new BadArgumentException("Please provide a valid resource type for data resource '" + schemaRecord.getSchemaId() + "'!\n"
                + "One of ['" + JSON + SCHEMA_SUFFIX + "', '" + XML + SCHEMA_SUFFIX + "']");
      }
    }
    String currentVersion = dataResource.getVersion();
    String schemaUrl = getSchemaDocumentUri(dataResource.getId(), currentVersion);
    schemaRecord.setVersion(currentVersion);
    schemaRecord.setPath(contentInformation.getContentUri());
    schemaRecord.setUrl(schemaUrl);

    return schemaRecord;
  }

  /**
   * Get creation date of data resource.
   *
   * @param dataResource data resource.
   * @return creation date.
   */
  public static final Instant getCreationDate(DataResource dataResource) {
    Instant creationDate = null;
    for (edu.kit.datamanager.repo.domain.Date d : dataResource.getDates()) {
      if (edu.kit.datamanager.repo.domain.Date.DATE_TYPE.CREATED.equals(d.getType())) {
        LOG.trace("Creation date entry found.");
        creationDate = d.getValue();
        break;
      }
    }
    return creationDate;
  }

  /**
   * Get String (URL) for accessing schema document via schemaId and version.
   *
   * @param schemaId schemaId.
   * @param version  version.
   * @return String for accessing schema document.
   */
  public static final String getSchemaDocumentUri(String schemaId, String version) {
    return WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(SchemaRegistryControllerImplV2.class).getSchemaDocumentById(schemaId, version, null, null)).toUri().toString();
  }

  /**
   * Get String (URL) for accessing metadata document via id and version.
   *
   * @param id      id.
   * @param version version.
   * @return URI for accessing schema document.
   */
  public static final URI getMetadataDocumentUri(String id, String version) {
    URI toUri = WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(MetadataControllerImplV2.class).getMetadataDocumentById(id, version, null, null)).toUri();
    if (toUri.getScheme() == null) {
      toUri = URI.create(baseUrl + toUri.toString());
    }
    return toUri;
  }

  /**
   * Query for data resources with provided specification or all if no
   * specification is provided.
   *
   * @param spec Specification of the data resources.
   * @param pgbl The pageable object containing pagination information.
   * @return Pageable Object holding all data resources fulfilling the
   * specification.
   */
  public static Page<DataResource> queryDataResources(Specification<DataResource> spec, Pageable pgbl) {
    Page<DataResource> records = null;
    try {
      records = spec != null ? dataResourceDao.findAll(spec, pgbl) : dataResourceDao.findAll(pgbl);
    } catch (Exception ex) {
      LOG.error("Error finding data resource records by specification!", ex);
      throw ex;
    }
    return records;
  }

  /**
   * Count the number of linked metadata documents per schema.
   *
   * @return A map with the number of linked metadata documents per schema.
   */
  public static Map<String, Long> collectDocumentsPerSchema() {
    Map<String, Long> documentsPerSchema = new HashMap<>();
    // Search for resource type of MetadataSchemaRecord
    Specification<DataResource> spec = DataResourceRecordUtil.findByMimetypes(null, null);
    // Ignore all records that are deleted or gone
    spec = DataResourceRecordUtil.findByStateWithAuthorization(spec, DataResource.State.FIXED, DataResource.State.VOLATILE);

    LOG.debug("Performing query for records.");
    Pageable pageable = PageRequest.of(0, 10);
    Page<DataResource> records = DataResourceRecordUtil.queryDataResources(spec, pageable);
    for (DataResource record : records.getContent()) {
      String schemaId = record.getId();
      // Get no of documents per schema
      spec = DataResourceRecordUtil.findBySchemaId(null, Arrays.asList(schemaId));
      // Ignore all records that are deleted or gone
      spec = DataResourceRecordUtil.findByStateWithAuthorization(spec, DataResource.State.FIXED, DataResource.State.VOLATILE);
      Page<DataResource> documents = DataResourceRecordUtil.queryDataResources(spec, pageable);
      documentsPerSchema.put(schemaId,
              documents.getTotalElements());
    }

    return documentsPerSchema;
  }

  public static List<Resource2FileVersion> getResource2FileVersions(String dataResourceId) {
    return resource2FileVersionDao.findByResourceIdOrderByVersionDesc(dataResourceId);
  }

  /**
   * Remove all entries from database and all related files from disc. (For
   * dataresources with state 'GONE' only.)
   *
   * @param dataResourceToRemove Identifier of the resource
   */
  public static void cleanUpDataResource(DataResource dataResourceToRemove) {
    if (dataResourceToRemove.getState() == DataResource.State.GONE) {
      LOG.trace("State 'GONE' detected. -> clean up resource");
      String dataResourceId = dataResourceToRemove.getId();

      List<String> uniqueIdentifiers = getUniqueIdentifiers(dataResourceToRemove);
      List<Resource2FileVersion> resource2FileVersions = getResource2FileVersions(dataResourceId);
      ContentInformation contentInformationByIdAndVersion = null;
      for (Resource2FileVersion resource2FileVersion : resource2FileVersions) {
        contentInformationByIdAndVersion = getContentInformationByIdAndVersion(schemaConfig, dataResourceId, resource2FileVersion.getVersion());
        String contentUri = contentInformationByIdAndVersion.getContentUri();
        LOG.trace("Try to remove version '{}' of '{}' -> file: '{}'...", resource2FileVersion.getVersion(), dataResourceId, contentUri);
        try {
          Path metadataDocumentPath = testForRegularFile(contentUri);
          Files.delete(metadataDocumentPath);
          LOG.trace("-> removed!");
        } catch (CustomInternalServerError | IOException ex) {
          LOG.error("Problem deleting '{}'", contentUri);
          LOG.error("Reason: ", ex);
        }
        cleanUpHelperTables(dataResourceId, contentUri);
      }
      schemaConfig.getContentInformationService().delete(contentInformationByIdAndVersion);

      LOG.trace("Delete data resource: '{}'", dataResourceToRemove.getId());
      dataResourceDao.delete(dataResourceToRemove);
      List<AllIdentifiers> findByIdentifierIn = allIdentifiersDao.findByIdentifierIn(uniqueIdentifiers);
      for (AllIdentifiers identifier : findByIdentifierIn) {
        LOG.trace("AllIdentifiers remove: '{}'", identifier);
        allIdentifiersDao.delete(identifier);
      }
      List<Resource2FileVersion> entities = getResource2FileVersions(dataResourceToRemove.getId());
      for (Resource2FileVersion entity : entities) {
        LOG.trace("Delete resource2FileVersion entry: '{}'", entity);
        resource2FileVersionDao.delete(entity);
      }
    }
  }

  private static void cleanUpHelperTables(String dataResourceId, String contentUri) {
    // if data resource is a schema there are some helper tables...
    List<SchemaUrl2Path> allSchemaIds = schemaUrl2PathDao.findBySchemaIdOrderByVersionDesc(dataResourceId);
    for (SchemaUrl2Path schemaRecord : allSchemaIds) {
      LOG.trace("Delete schemaRecord: '{}'", schemaRecord);
      schemaUrl2PathDao.delete(schemaRecord);
    }
    List<SchemaUrl2Path> findByPath = schemaUrl2PathDao.findByPath(contentUri);
    for (SchemaUrl2Path entity : findByPath) {
      schemaUrl2PathDao.delete(entity);
      LOG.trace("Delete url2Path: '{}'", entity);
    }
  }

  /**
   * Get all identifiers of a resource that have to be unique, e.g. primary and
   * alternate identifiers.
   *
   * @param resource The resource.
   * @return A list of identifiers.
   */
  private static List<String> getUniqueIdentifiers(DataResource resource) {
    List<String> identifiers = new ArrayList<>();
    identifiers.add(resource.getId());
    if (resource.getIdentifier() != null) {
      identifiers.add(resource.getIdentifier().getValue());
    }
    resource.getAlternateIdentifiers().forEach((alt) -> {
      identifiers.add(alt.getValue());
    });
    return identifiers;
  }

  /**
   * Test if String points to a regular file, which is readable.
   *
   * @param fileUri URI of file.
   * @return Path to File
   * @throws CustomInternalServerError File is not a regular file or not
   *                                   available or not readable.
   */
  public static Path testForRegularFile(String fileUri) throws CustomInternalServerError {
    Path documentPath = Paths.get(URI.create(fileUri));
    if (!Files.exists(documentPath) || !Files.isRegularFile(documentPath) || !Files.isReadable(documentPath)) {
      LOG.warn("Metadata document at path {} either does not exist or is no file or is not readable. Returning HTTP NOT_FOUND.", documentPath);
      throw new CustomInternalServerError("Metadata document on server either does not exist or is no file or is not readable.");
    }
    return documentPath;
  }

  public static boolean checkForEquality(Set<AclEntry> oldEntries, Set<AclEntry> newEntries) {
    boolean isEqual = false;
    HashSet<Integer> collectIndices = new HashSet<>();
    if (oldEntries == newEntries) {
      isEqual = true;
    } else {
      if (oldEntries != null && newEntries != null && oldEntries.size() == newEntries.size()) {
        isEqual = true;
        for (AclEntry newEntry : newEntries) {
          int index = 0;
          boolean match = false;
          for (AclEntry oldEntry : oldEntries) {
            if (newEntry.getSid() != null && newEntry.getSid().equals(oldEntry.getSid())) {
              if (newEntry.getPermission() != null && newEntry.getPermission().equals(oldEntry.getPermission())) {
                match = true;
                collectIndices.add(index);
                break;
              }
            }
            index++;
          }
          if (!match) {
            isEqual = false;
            break;
          }
        }
      }
    }
    return (isEqual && (collectIndices.isEmpty() || (collectIndices.size() == oldEntries.size())));
  }

  /**
   * Get all authorization identities.
   *
   * @return List with all identities.
   */
  private static List<String> getAllAuthorizationIdentities() {
    Authentication authentication = AuthenticationHelper.getAuthentication();
    List<String> authorizationIdentities = AuthenticationHelper.getAuthorizationIdentities();
    for (GrantedAuthority authority : authentication.getAuthorities()) {
      authorizationIdentities.add(authority.getAuthority());
    }
    return authorizationIdentities;
  }

  public static void saveNewSchemaRecord(SchemaUrl2Path schemaRecord) {
    if (schemaUrl2PathDao != null) {
      try {
        schemaRecord.setUrl(DataResourceRecordUtil.getSchemaDocumentUri(schemaRecord.getSchemaId(), schemaRecord.getVersion()));
        schemaUrl2PathDao.save(schemaRecord);
      } catch (Exception npe) {
        LOG.error("Can't save schema record: " + schemaRecord, npe);
      }
      LOG.trace("Schema record saved: {}", schemaRecord);
    }
  }

  private static void saveNewResource2FileVersion(DataResource resource, ContentInformation contentInformation) {
    if (resource2FileVersionDao != null) {
      try {
        Resource2FileVersion resource2FileVersion = new Resource2FileVersion();
        resource2FileVersion.setResourceId(resource.getId());
        resource2FileVersion.setVersion(resource.getVersion());
        resource2FileVersion.setFileVersion(contentInformation.getVersion());
        resource2FileVersionDao.findAll().forEach((record) -> LOG.trace("Existing resource2fileversion record: '{}'", record));
        for (Resource2FileVersion record : resource2FileVersionDao.findAll()) {
          LOG.trace("Existing resource2fileversion record: '{}'", record);
        }
        LOG.trace("New resource2fileversion record: '{}'", resource2FileVersion);
        resource2FileVersionDao.save(resource2FileVersion);
      } catch (Exception npe) {
        LOG.error("Can't save resource2fileversion record for resource: " + resource.getId() + " and file version: " + resource.getVersion(), npe);
      }
      LOG.trace("Resource2FileVersion record saved for resource id: {} and file version: {}", resource.getId(), resource.getVersion());
    }
  }

  public static String getPreviousVersion(DataResource resource) {
    SemanticVersion currentVersion = SemanticVersion.parse(resource.getVersion());
    String previousVersionString = getRecordByIdAndVersion(schemaConfig, resource.getId(), null).getVersion();
    SemanticVersion previousVersion = SemanticVersion.parse(previousVersionString);
    if (!previousVersion.isBefore(currentVersion)) {
      LOG.debug("There seems to be no previous version! Current version '{}' is not greater than previous version '{}'. Returning current version.", currentVersion, previousVersion);
      previousVersionString = resource.getVersion();
    }
    return previousVersionString;
  }

  public static DataResource check4VersionUpdate(DataResource resource, String oldVersion) {
    if (resource.getVersion() == null) {
      resource.setVersion(oldVersion);
    }
    SemanticVersion currentVersion = SemanticVersion.parse(oldVersion);
    String providedVersion = resource.getVersion();
    Optional<SemanticVersion> newVersion = SemanticVersion.tryParse(resource.getVersion());
    LOG.trace("Check version update: current version '{}', provided version '{}'", currentVersion, newVersion);
    if (newVersion.isEmpty() || newVersion.get().isAtMost(currentVersion)) {
      resource.setVersion(oldVersion);
      String message = "Version has to be incremented! Current version: '{}' provided version: '{}'.";
      LOG.trace(message, currentVersion, providedVersion);
      resource = DataResourceRecordUtil.incrementVersion(resource, SemanticVersion.INCREMENT_LEVEL.MAJOR);
    }
    return resource;
  }

  /**
   * Increment version of data resource by default level.
   * If no version is available, version '1.0.0' will be set.
   *
   * @param resource data resource which should be updated.
   * @return Updated data resource with incremented version.
   */
  public static DataResource incrementVersion(DataResource resource) {
    return incrementVersion(resource, SemanticVersion.INCREMENT_LEVEL.MAJOR);
  }

  /**
   * Increment version of data resource according to given increment level.
   *
   * @param resource       data resource which should be updated.
   * @param incrementLevel level which should be incremented.
   * @return Updated data resource with incremented version.
   */
  public static DataResource incrementVersion(DataResource resource, SemanticVersion.INCREMENT_LEVEL incrementLevel) {
    String version = resource.getVersion();
    if (version == null) {
      version = "1.0.0";
    }
    SemanticVersion currentVersion = SemanticVersion.parse(version);
    resource.setVersion(currentVersion.increment(incrementLevel).toString());

    return resource;
  }

  public static void setToken(String token) {
    DataResourceRecordUtil.guestToken = token;
  }

  /**
   * Set base URL for accessing instances.
   *
   * @param aBaseUrl the baseUrl to set
   */
  public static void setBaseUrl(String aBaseUrl) {
    baseUrl = aBaseUrl;
  }
}
