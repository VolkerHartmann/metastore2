/*
 * Copyright 2023 Karlsruhe Institute of Technology.
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

import edu.kit.datamanager.entities.Identifier;
import edu.kit.datamanager.entities.PERMISSION;
import edu.kit.datamanager.exceptions.BadArgumentException;
import edu.kit.datamanager.exceptions.CustomInternalServerError;
import edu.kit.datamanager.exceptions.UnprocessableEntityException;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord.SCHEMA_TYPE;
import edu.kit.datamanager.metastore2.domain.ResourceIdentifier;
import edu.kit.datamanager.metastore2.domain.SchemaUrl2Path;
import edu.kit.datamanager.metastore2.validation.IValidator;
import edu.kit.datamanager.metastore2.validation.impl.XmlValidator;
import edu.kit.datamanager.repo.domain.acl.AclEntry;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 *
 * @author hartmann-v
 */
public class SchemaRecordUtilTest {

  public SchemaRecordUtilTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Constructor
   */
  @Test
  public void testConstructor() {
    assertNotNull(new SchemaRecordUtil());
  }

  /**
   * Test of migrateToDataResource method, of class SchemaRecordUtil.
   */
  @Test
  public void testMigrateToDataResource() {
    System.out.println("migrateToDataResource");
    System.out.println("Test moved to SchemaRegistryControllerTest");
    // due to mandatory application properties.
  }

  @Test(expected = NullPointerException.class)
  public void testValidateResourceIdentifierNoType() {
    MetastoreConfiguration conf = new MetastoreConfiguration();
    ResourceIdentifier identifier = ResourceIdentifier.factoryResourceIdentifier("any", null);
    fail("Don't reach this line!");
  }

  @Test(expected = BadArgumentException.class)
  public void testValidateMetadataDocumentNull() throws IOException {
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setUrl("any");
    SchemaRecordUtil.validateMetadataDocument(conf, (InputStream) null, schemaRecord);
    fail("Don't reach this line!");
  }

  @Test(expected = BadArgumentException.class)
  public void testValidateMetadataDocumentEmpty() throws IOException {
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setUrl("any");
    InputStream emptyInputStream = new ByteArrayInputStream("".getBytes());
    SchemaRecordUtil.validateMetadataDocument(conf, emptyInputStream, schemaRecord);
    fail("Don't reach this line!");
  }

  @Test(expected = BadArgumentException.class)
  public void testValidateMetadataDocumentSchemaRecordNull() throws IOException {
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = null;
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
    SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
    fail("Don't reach this line!");
  }

  @Test(expected = BadArgumentException.class)
  public void testValidateMetadataDocumentSchemaRecordUriNull() throws IOException {
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
    SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
    fail("Don't reach this line!");
  }

  @Test(expected = BadArgumentException.class)
  public void testValidateMetadataDocumentSchemaRecordUriEmpty() throws IOException {
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setPath("   ");
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
    SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
    fail("Don't reach this line!");
  }

  @Test(expected = CustomInternalServerError.class)
  public void testValidateMetadataDocumentSchemaRecordUriNotExisting() throws IOException {
    // This test only works on Unix-like systems, as it uses the /dev directory which is not available on Windows.
    String os = System.getProperty("os.name").toLowerCase();
    Assume.assumeFalse(os.contains("win"));
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setPath("file:///non/existing/path/schema.json");
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
      SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
      fail("Don't reach this line!");
  }

  @Test(expected = CustomInternalServerError.class)
  public void testValidateMetadataDocumentSchemaRecordUriExistingButNotRegular() throws IOException {
    // This test only works on Unix-like systems, as it uses the /dev directory which is not available on Windows.
    String os = System.getProperty("os.name").toLowerCase();
    Assume.assumeFalse(os.contains("win"));
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setPath("file:///dev");
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
      SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
      fail("Don't reach this line!");
  }

  @Test(expected = CustomInternalServerError.class)
  public void testValidateMetadataDocumentSchemaRecordUriExistingButNotReadable() throws IOException {
    // This test only works on Unix-like systems, as it uses the /dev directory which is not available on Windows.
    String os = System.getProperty("os.name").toLowerCase();
    Assume.assumeFalse(os.contains("win"));
    // Create a temporary file which is not readable
    Path file = Files.createTempFile("notreadable", ".txt");
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("---------"));
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setPath(file.toUri().toString());
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
      SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
      fail("Don't reach this line!");
  }

  @Test(expected = UnprocessableEntityException.class)
  public void testValidateMetadataDocumentMimetypeIsNull() throws IOException {
    // This test only works on Unix-like systems, as it uses the /dev directory which is not available on Windows.
    MetastoreConfiguration conf = new MetastoreConfiguration();
    ArrayList<IValidator> validators = new ArrayList<>();
    validators.add(new XmlValidator());
    conf.setValidators(validators);
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    Path schemaPath = Path.of("./src/test/resources/examples/xml/example.xsd");
    schemaRecord.setPath(schemaPath.toUri().toString());
    schemaRecord.setMimetype(null);
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
    SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
  }

  @Test(expected = UnprocessableEntityException.class)
  public void testValidateMetadataDocumentMimetypeIsNullButInvalidSchema() throws IOException {
    // This test only works on Unix-like systems, as it uses the /dev directory which is not available on Windows.
    String os = System.getProperty("os.name").toLowerCase();
    Assume.assumeFalse(os.contains("win"));
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    Path schemaPath = Path.of("./src/test/resources/examples/anyContentWithoutSuffix");
    schemaRecord.setPath(schemaPath.toUri().toString());
    schemaRecord.setMimetype(null);
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
    SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
  }

  @Test(expected = UnprocessableEntityException.class)
  public void testValidateMetadataDocumentMimetypeIsNotAvailable() throws IOException {
    // This test only works on Unix-like systems, as it uses the /dev directory which is not available on Windows.
    String os = System.getProperty("os.name").toLowerCase();
    Assume.assumeFalse(os.contains("win"));
    MetastoreConfiguration conf = new MetastoreConfiguration();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    Path schemaPath = Path.of("./src/test/resources/examples/anyContentWithoutSuffix");
    schemaRecord.setPath(schemaPath.toUri().toString());
    schemaRecord.setMimetype("application/unknown");
    InputStream schemaDocument = new ByteArrayInputStream("any content".getBytes());
    conf.setValidators(new ArrayList<>());
    SchemaRecordUtil.validateMetadataDocument(conf, schemaDocument, schemaRecord);
  }

  @Test
  public void testIdentifierTypes() {
    MetadataSchemaRecord mr = new MetadataSchemaRecord();
    ResourceIdentifier.IdentifierType[] values = ResourceIdentifier.IdentifierType.values();
    for (ResourceIdentifier.IdentifierType item : values) {
      assertNotNull(item.value() + " is not defined in DataResource!", Identifier.IDENTIFIER_TYPE.valueOf(item.name()));
    }
  }

  @Test
  public void testFixRelativeUri() {
    String relativeUri = "schema1.json";
    String result = SchemaRecordUtil.fixRelativeURI(relativeUri);
    Assert.assertTrue(result.startsWith("file:/"));
    Assert.assertTrue(result.endsWith(relativeUri));
  }

  @Test(expected = FileSystemNotFoundException.class)
  public void testFixRelativeUriWithUnknownProtocol() {
    String relativeUri = "unknown:/schema1.json";
    String result = SchemaRecordUtil.fixRelativeURI(relativeUri);
    Assert.fail("This line should not be executed!");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFixRelativeUriWithInvalidUri() {
    String relativeUri = "file:/// schema1.json";
    String result = SchemaRecordUtil.fixRelativeURI(relativeUri);
    Assert.fail("This line should not be executed!");
  }
  private MetadataSchemaRecord buildMSR(Set<AclEntry> aclEntry, String comment, Instant creationDate, String definition,
          String eTag, String label, Instant update, boolean doNotSync, String mimetype, String pid, String schemaDocument,
          String schemaHash, String schemaId, String version, SCHEMA_TYPE type) {
    MetadataSchemaRecord msr = new MetadataSchemaRecord();
    msr.setAcl(aclEntry);
    msr.setComment(comment);
    msr.setCreatedAt(creationDate);
    msr.setDefinition(definition);
    msr.setETag(eTag);
    msr.setLabel(label);
    msr.setLastUpdate(update);
    msr.setDoNotSync(doNotSync);
    msr.setMimeType(mimetype);
    msr.setPid(ResourceIdentifier.factoryUrlResourceIdentifier(pid));
    msr.setSchemaDocumentUri(schemaDocument);
    msr.setSchemaHash(schemaHash);
    msr.setSchemaId(schemaId);
    msr.setSchemaVersion(version);
    msr.setType(type);

    return msr;
 }

  private AclEntry createEntry(Long id, PERMISSION permission, String sid) {
    AclEntry entry = new AclEntry();
    entry.setId(id);
    entry.setPermission(permission);
    entry.setSid(sid);
    return entry;

  }
}
