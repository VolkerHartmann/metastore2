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

import edu.kit.datamanager.exceptions.BadArgumentException;
import edu.kit.datamanager.exceptions.CustomInternalServerError;
import edu.kit.datamanager.exceptions.UnprocessableEntityException;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.domain.SchemaUrl2Path;
import edu.kit.datamanager.metastore2.validation.IValidator;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class for handling json documents
 */
public class SchemaRecordUtil {

  /**
   * Logger for messages.
   */
  private static final Logger LOG = LoggerFactory.getLogger(SchemaRecordUtil.class);

  SchemaRecordUtil() {
    //Utility class
  }

  /**
   * Validate metadata document with given schema. In case of an error a runtime
   * exception is thrown.
   *
   * @param metastoreProperties Configuration properties.
   * @param inputStream         Document to validate.
   * @param schemaRecord        Record of the schema.
   */
  public static void validateMetadataDocument(MetastoreConfiguration metastoreProperties,
                                              InputStream inputStream,
                                              SchemaUrl2Path schemaRecord) throws IOException {
    LOG.trace("validateMetadataInputStream {},{}, {}", metastoreProperties, schemaRecord, inputStream);

    if (schemaRecord == null || schemaRecord.getPath() == null || schemaRecord.getPath().trim().isEmpty()) {
      String message = "Missing or invalid schema record. Returning HTTP BAD_REQUEST.";
      LOG.error(message + " -> '{}'", schemaRecord);
      throw new BadArgumentException(message);
    }
    LOG.trace("Checking local schema file.");
    Path schemaDocumentPath = Paths.get(URI.create(schemaRecord.getPath()));

    if (!Files.exists(schemaDocumentPath) || !Files.isRegularFile(schemaDocumentPath) || !Files.isReadable(schemaDocumentPath)) {
      LOG.error("Schema document with schemaId '{}'at path {} either does not exist or is no file or is not readable.", schemaRecord.getSchemaId(), schemaDocumentPath);
      throw new CustomInternalServerError("Schema document on server either does not exist or is no file or is not readable.");
    }
    LOG.trace("obtain validator for type");
    IValidator applicableValidator;
    if (schemaRecord.getMimetype() == null) {
      byte[] schemaDocument = FileUtils.readFileToByteArray(schemaDocumentPath.toFile());
      applicableValidator = getValidatorForRecord(metastoreProperties, schemaRecord, schemaDocument);
    } else {
      applicableValidator = getValidatorForRecord(metastoreProperties, schemaRecord, null);
    }

    if (applicableValidator == null) {
      String message = "No validator found for schema type " + schemaRecord.getMimetype();
      LOG.error(message);
      throw new UnprocessableEntityException(message);
    } else {
      LOG.trace("Validator found.");

      LOG.trace("Performing validation of metadata document using schema {}, version {} and validator {}.", schemaRecord.getSchemaId(), schemaRecord.getVersion(), applicableValidator);
      if (!applicableValidator.validateMetadataDocument(schemaDocumentPath.toFile(), inputStream)) {
        LOG.warn("Metadata document validation failed. -> " + applicableValidator.getErrorMessage());
        throw new UnprocessableEntityException(applicableValidator.getErrorMessage());
      }
    }
    LOG.trace("Metadata document validation succeeded.");
  }

  public static IValidator getValidatorForRecord(MetastoreConfiguration metastoreProperties, SchemaUrl2Path schemaRecord, byte[] schemaDocument) {
    String mimeType = schemaRecord != null ? schemaRecord.getMimetype() : null;
    return getValidatorForRecord(metastoreProperties, mimeType, schemaDocument);
  }

  public static IValidator getValidatorForRecord(MetastoreConfiguration metastoreProperties, String mimeType, byte[] schemaDocument) {
    IValidator applicableValidator = null;
    //obtain/guess record type
    if (mimeType == null) {
      mimeType = SchemaUtils.guessMimetype(schemaDocument);
      if (mimeType == null) {
        String message = "Unable to detect schema type automatically. Please provide a valid type";
        LOG.error(message);
        throw new UnprocessableEntityException(message);
      } else {
        LOG.debug("Automatically detected schema type {}.", mimeType);
      }
    }
    for (IValidator validator : metastoreProperties.getValidators()) {
      if (validator.supportsMimetype(mimeType)) {
        applicableValidator = validator.getInstance();
        LOG.trace("Found validator for schema: '{}'", mimeType);
        break;
      }
    }
    return applicableValidator;
  }


  /**
   * Fix relative URI.
   *
   * @param uri (relative) URI
   * @return absolute URL
   */
  public static String fixRelativeURI(String uri) {
    String returnValue = null;
    try {
      URI urig = URI.create(uri);
      if (urig.isAbsolute()) {
        returnValue = Paths.get(new URI(uri)).toAbsolutePath().toUri().toURL().toString();
      } else {
        returnValue = Paths.get(uri).toFile().toURI().toURL().toString();
      }
    } catch (URISyntaxException | MalformedURLException ex) {
      LOG.error("Error fixing URI '" + uri + "'", ex);
    }
    LOG.trace("Fix URI '{}' -> '{}'", uri, returnValue);
    return returnValue;
  }

}
