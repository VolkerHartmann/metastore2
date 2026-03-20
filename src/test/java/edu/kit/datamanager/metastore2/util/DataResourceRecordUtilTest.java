/*
 * Copyright 2020 hartmann-v.
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
import edu.kit.datamanager.exceptions.BadArgumentException;
import edu.kit.datamanager.exceptions.CustomInternalServerError;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.ISchemaUrl2PathDao;
import edu.kit.datamanager.metastore2.domain.SchemaUrl2Path;
import edu.kit.datamanager.repo.dao.IAllIdentifiersDao;
import edu.kit.datamanager.repo.dao.IContentInformationDao;
import edu.kit.datamanager.repo.dao.IDataResourceDao;
import edu.kit.datamanager.repo.domain.ContentInformation;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.repo.domain.RelatedIdentifier;
import edu.kit.datamanager.repo.domain.acl.AclEntry;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.restdocs.JUnitRestDocumentation;
import static org.springframework.restdocs.mockmvc.MockMvcRestDocumentation.documentationConfiguration;
import org.springframework.security.test.context.support.WithSecurityContextTestExecutionListener;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.test.context.web.ServletTestExecutionListener;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.multipart.MultipartFile;

/**
 *
 * @author hartmann-v
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@EntityScan("edu.kit.datamanager")
@EnableJpaRepositories("edu.kit.datamanager")
@ComponentScan({"edu.kit.datamanager"})
@AutoConfigureMockMvc
@TestExecutionListeners(listeners = {ServletTestExecutionListener.class,
  DependencyInjectionTestExecutionListener.class,
  DirtiesContextTestExecutionListener.class,
  TransactionalTestExecutionListener.class,
  WithSecurityContextTestExecutionListener.class})
@ActiveProfiles("test")
@TestPropertySource(properties = {"server.port=41439"})
@TestPropertySource(properties = {"spring.datasource.url=jdbc:h2:mem:db_schema_drru;DB_CLOSE_DELAY=-1;MODE=LEGACY;NON_KEYWORDS=VALUE"})
@TestPropertySource(properties = {"metastore.schema.schemaFolder=file:///tmp/metastore2/v2/drru/schema"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DataResourceRecordUtilTest {

  private static final String TEMP_DIR_4_ALL = "/tmp/metastore2/drru/";
  private static final String TEMP_DIR_4_SCHEMAS = TEMP_DIR_4_ALL + "schema/";
  private MockMvc mockMvc;
  @Autowired
  private WebApplicationContext context;

  @Autowired
  private IDataResourceDao dataResourceDao;
  @Autowired
  private ISchemaUrl2PathDao schemaUrl2PathDao;
  @Autowired
  private IContentInformationDao contentInformationDao;
  @Autowired
  private IAllIdentifiersDao allIdentifiersDao;

  @Autowired
  private MetastoreConfiguration schemaConfig;
  @Rule
  public JUnitRestDocumentation restDocumentation = new JUnitRestDocumentation();

  public DataResourceRecordUtilTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {

    System.out.println("------DataResourceRecordUtilTest--------------------");
    System.out.println("------" + this.schemaConfig);
    System.out.println("------------------------------------------------------");
    contentInformationDao.deleteAll();
    dataResourceDao.deleteAll();
    schemaUrl2PathDao.deleteAll();
    allIdentifiersDao.deleteAll();
    try {
      try (Stream<Path> walk = Files.walk(Paths.get(URI.create("file://" + TEMP_DIR_4_SCHEMAS)))) {
        walk.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
      }
      Paths.get(TEMP_DIR_4_SCHEMAS).toFile().mkdir();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.context)
            .apply(springSecurity())
            .apply(documentationConfiguration(this.restDocumentation))
            .build();
    SchemaUrl2Path schemaRecord = new SchemaUrl2Path();
    schemaRecord.setSchemaId("test");
    schemaRecord.setVersion("1.0.0");
    schemaRecord.setMimetype(MediaType.APPLICATION_JSON_VALUE);
    schemaRecord.setPath("anyPath");
    schemaRecord.setUrl("anyUrl1");
    schemaUrl2PathDao.save(schemaRecord);
    SchemaUrl2Path schemaRecord2 = new SchemaUrl2Path();
    schemaRecord2.setSchemaId("test");
    schemaRecord2.setVersion("2.0.0");
    schemaRecord2.setMimetype(MediaType.APPLICATION_JSON_VALUE);
    schemaRecord2.setPath("anyPath");
    schemaRecord2.setUrl("anyUrl2");
    schemaUrl2PathDao.save(schemaRecord2);
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of downloadResource method, of class GemmaMapping.
   */
  @Test
  public void testConstructor() {
    System.out.println("testConstructor");
    assertNotNull(new DataResourceRecordUtil());
  }

  @Test
  public void testAddProvenanceWithVersion1() {
    System.out.println("testAddProvenanceWithVersion1");
    DataResource factoryNewDataResource = DataResource.factoryNewDataResource();
    Set<RelatedIdentifier> relatedIdentifiers = factoryNewDataResource.getRelatedIdentifiers();
    factoryNewDataResource.setVersion("1.0.0");
    DataResourceRecordUtil.addProvenance(factoryNewDataResource);
    assertEquals(relatedIdentifiers, factoryNewDataResource.getRelatedIdentifiers());
  }

  @Test
  public void testMergeIdenticalAcls() {
    System.out.println("testMergeIdenticalAcls");
    Set<AclEntry> acls = new HashSet<>();
    Set<AclEntry> mergeAcl = DataResourceRecordUtil.mergeAcl(acls, acls);
    assertEquals(acls, mergeAcl);
  }

  @Test
  public void testMergeNullAcls() {
    System.out.println("testMergeNullAcls");
    Set<AclEntry> mergeAcl = DataResourceRecordUtil.mergeAcl(null, null);
    assertNotNull(mergeAcl);
    assertTrue(mergeAcl.isEmpty());
  }

  @Test
  public void testFixSchemaUrl() {
    System.out.println("testFixSchemaUrl");
    DataResourceRecordUtil.fixSchemaUrl((RelatedIdentifier) null);
    RelatedIdentifier ri1 = RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "test" + DataResourceRecordUtil.SCHEMA_VERSION_SEPARATOR + "1.0.0", null, null);
    ri1.setIdentifierType(Identifier.IDENTIFIER_TYPE.INTERNAL);
    RelatedIdentifier ri2 = RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "test", null, null);
    ri2.setIdentifierType(Identifier.IDENTIFIER_TYPE.INTERNAL);
    DataResourceRecordUtil.fixSchemaUrl(ri2);
    assertTrue("Found second version", ri2.getValue().endsWith("anyUrl2"));
    DataResourceRecordUtil.fixSchemaUrl(ri1);
    assertTrue("Found first version", ri1.getValue().endsWith("anyUrl1"));
  }

  @Test(expected = CustomInternalServerError.class)
  public void testFixSchemaUrlWrongFormat() {
    System.out.println("testFixSchemaUrl");
    DataResourceRecordUtil.fixSchemaUrl((RelatedIdentifier) null);
    RelatedIdentifier ri1 = RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "test" + DataResourceRecordUtil.SCHEMA_VERSION_SEPARATOR + "1" + DataResourceRecordUtil.SCHEMA_VERSION_SEPARATOR + "2", null, null);
    ri1.setIdentifierType(Identifier.IDENTIFIER_TYPE.INTERNAL);
    DataResourceRecordUtil.fixSchemaUrl(ri1);
    // following line shouldn't be reached
    fail();
  }

  @Test
  public void testvalidateRelatedResources4MetadataDocuments() {
    System.out.println("testvalidateRelatedResources4MetadataDocuments");
    try {
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(null);
      fail();
    } catch (BadArgumentException bae) {
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
    }
    try {
      DataResource hasNoRelatedIdentifier = DataResource.factoryNewDataResource();
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(hasNoRelatedIdentifier);
      fail();
    } catch (BadArgumentException bae) {
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
    }
    try {
      DataResource hasNeitherSchemaNorDataResource = DataResource.factoryNewDataResource();
      hasNeitherSchemaNorDataResource.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(RelatedIdentifier.RELATION_TYPES.IS_CITED_BY, "citation", null, null));
      hasNeitherSchemaNorDataResource.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(RelatedIdentifier.RELATION_TYPES.IS_DOCUMENTED_BY, "documentation", null, null));
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(hasNeitherSchemaNorDataResource);
      fail();
    } catch (BadArgumentException bae) {
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
    }
    try {
      DataResource hasTwoDataResourcesButNoSchema = DataResource.factoryNewDataResource();
      hasTwoDataResourcesButNoSchema.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "first data", null, null));
      hasTwoDataResourcesButNoSchema.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "second data", null, null));
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(hasTwoDataResourcesButNoSchema);
      fail();
    } catch (BadArgumentException bae) {
      assertFalse("Multiple '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "' should be allowed!", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
    }
    try {
      DataResource hasTwoSchemasAndNoDataResource = DataResource.factoryNewDataResource();
      hasTwoSchemasAndNoDataResource.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, "first schema", null, null));
      hasTwoSchemasAndNoDataResource.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, "second schema", null, null));
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(hasTwoSchemasAndNoDataResource);
      fail();
    } catch (BadArgumentException bae) {
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
    }
    try {
      DataResource hasOneSchemaAndNoDataResource = DataResource.factoryNewDataResource();
      hasOneSchemaAndNoDataResource.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, "first schema", null, null));
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(hasOneSchemaAndNoDataResource);
      fail();
    } catch (BadArgumentException bae) {
      assertFalse("Error should not contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
    }
    try {
      DataResource hasSchemaAndTwoDataResources = DataResource.factoryNewDataResource();
      hasSchemaAndTwoDataResources.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, "first schema", null, null));
      hasSchemaAndTwoDataResources.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "first data", null, null));
      hasSchemaAndTwoDataResources.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "second data", null, null));
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(hasSchemaAndTwoDataResources);
      assertTrue(true);
    } catch (BadArgumentException bae) {
      assertFalse("Error should not contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
      assertFalse("Multiple '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "' should be allowed!", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
      fail();
    }
    try {
      DataResource hasTwoSchemasAndTwoDataResources = DataResource.factoryNewDataResource();
      hasTwoSchemasAndTwoDataResources.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, "first schema", null, null));
      hasTwoSchemasAndTwoDataResources.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, "second schema", null, null));
      hasTwoSchemasAndTwoDataResources.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "first data", null, null));
      hasTwoSchemasAndTwoDataResources.getRelatedIdentifiers().add(RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE, "second data", null, null));
      DataResourceRecordUtil.validateRelatedResources4MetadataDocuments(hasTwoSchemasAndTwoDataResources);
      fail();
    } catch (BadArgumentException bae) {
      assertTrue("Error should contain '" + DataResourceRecordUtil.RELATED_SCHEMA_TYPE + "'", bae.getMessage().contains(DataResourceRecordUtil.RELATED_SCHEMA_TYPE.name()));
      assertFalse("Multiple '" + DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE + "' should be allowed!", bae.getMessage().contains(DataResourceRecordUtil.RELATED_DATA_RESOURCE_TYPE.name()));
    }
  }

  @Test(expected = CustomInternalServerError.class)
  public void testCheckDocumentForChangesWithNoContentInformation() {
    boolean result = DataResourceRecordUtil.checkDocumentForChanges(null, null);
    fail();
  }

  @Test(expected = BadArgumentException.class)
  public void testCheckDocumentForChangesWithInvalidContentInformation() {
    ContentInformation ci = new ContentInformation();
    ci.setContentUri("file:///tmp/somethingTotallyStrange");
    MultipartFile mpf = new MockMultipartFile("hallo.txt", "noContent".getBytes());
    boolean result = DataResourceRecordUtil.checkDocumentForChanges(ci, mpf);
    fail("Should have thrown BadArgumentException because content URI is not valid Result: " + result);
  }
  @Test(expected = NullPointerException.class)
  public void testIncrementVersionAllNull() {
    DataResourceRecordUtil.incrementVersion(null);
  }
  @Test
  public void testIncrementVersionWithoutVersion() {
    DataResource dr = DataResource.factoryNewDataResource();
    DataResource dr2 = DataResourceRecordUtil.incrementVersion(dr);
    assertEquals("2.0.0", dr2.getVersion());
    DataResource dr3 = DataResourceRecordUtil.incrementVersion(dr, SemanticVersion.INCREMENT_LEVEL.MINOR);
    assertEquals("2.1.0", dr3.getVersion());
    DataResource dr4 = DataResourceRecordUtil.incrementVersion(dr, SemanticVersion.INCREMENT_LEVEL.PATCH);
    assertEquals("2.1.1", dr4.getVersion());
  }
  @Test(expected = NullPointerException.class)
  public void testIncrementVersionWithLevelNull() {
    DataResource dr = DataResource.factoryNewDataResource();
    dr.setVersion("1.2.3");
    DataResourceRecordUtil.incrementVersion(dr, null);
    assertEquals("2.0.0", dr.getVersion());
  }

  @Test
  public void testCheckAccessRightsWithNull() {
    boolean access = DataResourceRecordUtil.checkAccessRights(null, true);
    assertTrue(access);
  }
}