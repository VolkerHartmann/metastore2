/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.metastore2.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.kit.datamanager.entities.PERMISSION;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.ISchemaRecordDao;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import edu.kit.datamanager.metastore2.domain.SchemaRecord;
import edu.kit.datamanager.metastore2.util.DataResourceRecordUtil;
import edu.kit.datamanager.repo.dao.IAllIdentifiersDao;
import edu.kit.datamanager.repo.dao.IContentInformationDao;
import edu.kit.datamanager.repo.dao.IDataResourceDao;
import edu.kit.datamanager.repo.domain.*;
import edu.kit.datamanager.repo.domain.Date;
import edu.kit.datamanager.repo.domain.acl.AclEntry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springdoc.core.customizers.DataRestRouterOperationCustomizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.restdocs.JUnitRestDocumentation;
import org.springframework.security.test.context.support.WithSecurityContextTestExecutionListener;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static org.springframework.restdocs.mockmvc.MockMvcRestDocumentation.documentationConfiguration;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Test for the JsonSchemaRegistryController.
 * This test checks the creation, retrieval, and update of JSON schema records.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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
@TestPropertySource(properties = {"spring.datasource.url=jdbc:h2:mem:db_schema_json;DB_CLOSE_DELAY=-1;MODE=LEGACY;NON_KEYWORDS=VALUE"})
@TestPropertySource(properties = {"metastore.schema.schemaFolder=file:///tmp/metastore2/jsontest/schema"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class JsonSchemaRegistryControllerTestV2 {

  private final static String TEMP_DIR_4_ALL = "/tmp/metastore2/jsontest/";
  private final static String TEMP_DIR_4_SCHEMAS = TEMP_DIR_4_ALL + "schema/";
  private static final String INVALID_SCHEMA_ID = "invalid/json";
  private final static String JSON_SCHEMA = "{\n"
          + "    \"$schema\": \"https://json-schema.org/draft/2019-09/schema\",\n"
          + "    \"$id\": \"http://www.example.org/schema/json\",\n"
          + "    \"type\": \"object\",\n"
          + "    \"title\": \"Json schema for tests\",\n"
          + "    \"default\": {},\n"
          + "    \"required\": [\n"
          + "        \"title\",\n"
          + "        \"date\"\n"
          + "    ],\n"
          + "    \"properties\": {\n"
          + "        \"title\": {\n"
          + "            \"type\": \"string\",\n"
          + "            \"title\": \"Title\",\n"
          + "            \"description\": \"Title of object.\"\n"
          + "        },\n"
          + "        \"date\": {\n"
          + "            \"type\": \"string\",\n"
          + "            \"format\": \"date\",\n"
          + "            \"pattern\": \"^[0-9]{4}-[01][0-9]-[0-3][0-9]$\",\n"
          + "            \"title\": \"Date\",\n"
          + "            \"description\": \"Date of object\"\n"
          + "        }\n"
          + "    },\n"
          + "    \"additionalProperties\": false\n"
          + "}";
  private final static String JSON_SCHEMA_V2 = "{\n"
          + "    \"$schema\": \"https://json-schema.org/draft/2019-09/schema\",\n"
          + "    \"$id\": \"http://www.example.org/schema/json\",\n"
          + "    \"type\": \"object\",\n"
          + "    \"title\": \"Json schema for tests\",\n"
          + "    \"default\": {},\n"
          + "    \"required\": [\n"
          + "        \"title\",\n"
          + "        \"date\"\n"
          + "    ],\n"
          + "    \"properties\": {\n"
          + "        \"title\": {\n"
          + "            \"type\": \"string\",\n"
          + "            \"title\": \"Title\",\n"
          + "            \"description\": \"Title of object.\"\n"
          + "        },\n"
          + "        \"date\": {\n"
          + "            \"type\": \"string\",\n"
          + "            \"format\": \"date\",\n"
          + "            \"pattern\": \"^[0-9]{4}-[01][0-9]-[0-3][0-9]$\",\n"
          + "            \"title\": \"Date\",\n"
          + "            \"description\": \"Date of object\"\n"
          + "        },\n"
          + "        \"note\": {\n"
          + "            \"type\": \"string\",\n"
          + "            \"title\": \"Note\",\n"
          + "            \"description\": \"Additonal information about object.\"\n"
          + "        }\n"
          + "    },\n"
          + "    \"additionalProperties\": false\n"
          + "}";

  private final static String JSON_SCHEMA4UPDATE = "{\n"
          + "    \"type\": \"object\", "
          + "    \"properties\": "
          + "    { "
          + "        \"title\": "
          + "        { "
          + "            \"type\": \"string\", "
          + "            \"title\": \"Title\", "
          + "            \"description\": \"Title of object.\" "
          + "        } "
          + "    } "
          + "}";
  private final static String JSON_DOCUMENT = "{\"title\":\"any string\",\"date\": \"2020-10-16\"}";
  private final static String INVALID_JSON_DOCUMENT = "{\"title\":\"any string\",\"date\":\"2020-10-16T10:13:24\"}";
  private final static String DC_DOCUMENT = "<?xml version='1.0' encoding='utf-8'?>\n"
          + "<oai_dc:dc xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">\n"
          + "  <dc:creator>Carbon, Seth</dc:creator>\n"
          + "  <dc:creator>Mungall, Chris</dc:creator>\n"
          + "  <dc:date>2018-07-02</dc:date>\n"
          + "  <dc:description>Archival bundle of GO data release.</dc:description>\n"
          + "  <dc:identifier>https://zenodo.org/record/3477535</dc:identifier>\n"
          + "  <dc:identifier>10.5281/zenodo.3477535</dc:identifier>\n"
          + "  <dc:identifier>oai:zenodo.org:3477535</dc:identifier>\n"
          + "  <dc:relation>doi:10.5281/zenodo.1205166</dc:relation>\n"
          + "  <dc:relation>url:https://zenodo.org/communities/gene-ontology</dc:relation>\n"
          + "  <dc:relation>url:https://zenodo.org/communities/zenodo</dc:relation>\n"
          + "  <dc:rights>info:eu-repo/semantics/openAccess</dc:rights>\n"
          + "  <dc:rights>http://creativecommons.org/licenses/by/4.0/legalcode</dc:rights>\n"
          + "  <dc:title>Gene Ontology Data Archive</dc:title>\n"
          + "  <dc:type>info:eu-repo/semantics/other</dc:type>\n"
          + "  <dc:type>dataset</dc:type>\n"
          + "</oai_dc:dc>";

  private MockMvc mockMvc;
  @Autowired
  private WebApplicationContext context;
  @Autowired
  private IDataResourceDao dataResourceDao;
  @Autowired
  private ISchemaRecordDao schemaRecordDao;
  @Autowired
  private IContentInformationDao contentInformationDao;
  @Autowired
  private IAllIdentifiersDao allIdentifiersDao;
  @Autowired
  private MetastoreConfiguration schemaConfig;
  @Rule
  public JUnitRestDocumentation restDocumentation = new JUnitRestDocumentation();

  @Before
  public void setUp() throws Exception {
    System.out.println("------JsonSchemaRegistryControllerTestV2----------------");
    System.out.println("------" + this.schemaConfig);
    System.out.println("------------------------------------------------------");
    contentInformationDao.deleteAll();
    dataResourceDao.deleteAll();
    schemaRecordDao.deleteAll();
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
  }

  @Test
  public void testCreateSchemaRecord() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcls(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithoutMimetype() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json_2");
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcls(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithoutContentType() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcls(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", null, JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithLocationUri() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcls(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andExpect(redirectedUrlPattern("http://*:*/**/" + record.getId() + "?version=1")).andReturn();
    String locationUri = result.getResponse().getHeader("Location");
    result.getResponse().getContentAsString();

    // URL should point to API v2. Therefor accept header is not allowed. 
    this.mockMvc.perform(get(locationUri).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk());
  }

  @Test
  public void testCreateInvalidSchemaRecord() throws Exception {
    DataResource record = new DataResource();
    record.setId(INVALID_SCHEMA_ID);
    record.setResourceType(ResourceType.createResourceType(DataResourceRecordUtil.JSON_SCHEMA_TYPE));
    record.getFormats().add(MediaType.APPLICATION_JSON.toString());
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateInvalidDataResource() throws Exception {
    String wrongTypeJson = "{\"schemaId\":\"dc\",\"type\":\"Something totally strange!\"}";

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", wrongTypeJson.getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.xsd", "application/json", JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
    String wrongFormatJson = "<metadata><schemaId>dc</schemaId><type>XML</type></metadata>";
    recordFile = new MockMultipartFile("record", "record.json", "application/json", wrongFormatJson.getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();

  }

  @Test
  public void testCreateEmptyDataResource() throws Exception {

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", (byte[]) null);
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.xsd", "application/xml", JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();

    recordFile = new MockMultipartFile("record", "record.json", "application/json", " ".getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }
  // @Test 
  public void testCreateSchemaRecordFromExternal() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());
    RequestPostProcessor rpp = new RequestPostProcessor() {
      @Override
      public MockHttpServletRequest postProcessRequest(MockHttpServletRequest mhsr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile).with(remoteAddr("any.external.domain"))).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  //@Test @ToDo Set external remote address.
  public void testCreateSchemaRecordUpdateFromExternal() throws Exception {
    DataResource record = new DataResource();
    record.setId("my_jsonExt");
    record.setResourceType(ResourceType.createResourceType(DataResourceRecordUtil.JSON_SCHEMA_TYPE));
    record.getFormats().add(MediaType.APPLICATION_JSON.toString());
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile).with(remoteAddr("any.domain.com"))).andDo(print()).andExpect(status().isCreated()).andReturn();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile).with(remoteAddr("www.google.com"))).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWrongType() throws Exception {
    // Create XML schema record with JSON schema content
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4XmlSchema("my_json");
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordGuessingType() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    record.setResourceType(null);
    record.getFormats().clear();
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    MvcResult res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();
    record = mapper.readValue(res.getResponse().getContentAsString(), DataResource.class);
    Assert.assertEquals(DataResourceRecordUtil.JSON_SCHEMA_TYPE, record.getResourceType().getValue());
  }

  @Test
  public void testCreateSchemaRecordGuessingTypeFails() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    record.setResourceType(null);
    record.getFormats().clear();
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "?".getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();

  }

  @Test
  public void testCreateSchemaRecordWithBadSchema() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "<>".getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithEmptySchema() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "".getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithoutRecord() throws Exception {
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithoutSchema() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json");
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithBadRecord() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    // No schemaId
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("any_id");
    record.setId(null);

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateTwoVersionsOfSchemaRecord() throws Exception {
    DataResource record = SchemaRegistryControllerTestV2.createDataResource4JsonSchema("my_json_with_version");
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    MvcResult res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();

    DataResource result = mapper.readValue(res.getResponse().getContentAsString(), DataResource.class);
    Assert.assertEquals(result.getVersion(), Long.toString(1));

    res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isConflict()).andReturn();
  }

  @Test
  public void testGetSchemaRecordByIdWithoutVersion() throws Exception {
    ingestSchemaRecord();

    MvcResult res = this.mockMvc.perform(get("/api/v2/schemas/json").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    DataResource result = map.readValue(res.getResponse().getContentAsString(), DataResource.class);
    Assert.assertNotNull(result);
    Assert.assertEquals("json", result.getId());
  }

  @Test
  public void testGetSchemaRecordByIdWithVersion() throws Exception {
    ingestSchemaRecord();

    MvcResult res = this.mockMvc.perform(get("/api/v2/schemas/json").param("version", "1").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    DataResource result = map.readValue(res.getResponse().getContentAsString(), DataResource.class);
    Assert.assertNotNull(result);
    Assert.assertEquals("json", result.getId());
//    Assert.assertNotEquals("file:///tmp/json.json", result.getSchemaDocumentUri());
  }

  @Test
  public void testGetSchemaRecordByIdWithInvalidId() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(get("/api/v2/schemas/nosj").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isNotFound()).andReturn();
  }

  @Test
  public void testGetSchemaRecordByIdWithInvalidVersion() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(get("/api/v2/schemas/json").param("version", "13").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().is4xxClientError()).andReturn();
  }

  @Test
  public void testFindRecordsBySchemaId() throws Exception {
    ingestSchemaRecord();
    MvcResult res = this.mockMvc.perform(get("/api/v2/schemas/").param("schemaId", "json").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    DataResource[] result = map.readValue(res.getResponse().getContentAsString(), DataResource[].class);

    Assert.assertTrue(result.length > 0);
  }

  @Test
  public void testFindRecordsByMimeType() throws Exception {
    ingestSchemaRecord();
    MvcResult res = this.mockMvc.perform(get("/api/v2/schemas/").param("mimeType", MediaType.APPLICATION_JSON.toString())).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    DataResource[] result = map.readValue(res.getResponse().getContentAsString(), DataResource[].class);

    Assert.assertEquals(1, result.length);
  }

  @Test
  public void testFindRecordsByInvalidMimeType() throws Exception {
    ingestSchemaRecord();
    MvcResult res = this.mockMvc.perform(get("/api/v2/schemas/").param("mimeType", "invalid")).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    DataResource[] result = map.readValue(res.getResponse().getContentAsString(), DataResource[].class);

    Assert.assertEquals(0, result.length);
  }

  @Test
  public void testFindRecordsByUnknownSchemaId() throws Exception {
    ingestSchemaRecord();
    MvcResult res = this.mockMvc.perform(get("/api/v2/schemas/").
            param("schemaId", "cd")).
            andDo(print()).
            andExpect(status().isOk()).
            andReturn();
    ObjectMapper map = new ObjectMapper();
    DataResource[] result = map.readValue(res.getResponse().getContentAsString(), DataResource[].class);

    Assert.assertEquals(0, result.length);
  }

  @Test
  public void testGetSchemaDocument() throws Exception {
    ingestSchemaRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/json")).andDo(print()).andExpect(status().isOk()).andReturn();
    String content = result.getResponse().getContentAsString();

    Assert.assertEquals(JSON_SCHEMA, content);
  }

  @Test
  public void testGetSchemaDocumentWithMissingSchemaFile() throws Exception {
    ingestSchemaRecord();
    String contentUri = contentInformationDao.findAll(PageRequest.of(0, 2)).getContent().get(0).getContentUri();

    //delete schema file
    URI uri = new URI(contentUri);
    Files.delete(Paths.get(uri));

    this.mockMvc.perform(get("/api/v2/schemas/json")).andDo(print()).andExpect(status().isInternalServerError()).andReturn();
  }

  @Test
  public void testValidate() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate").file("document", JSON_DOCUMENT.getBytes())).andDo(print()).andExpect(status().isNoContent()).andReturn();
  }

  @Test
  public void testValidateUnknownVersion() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate?version=666").file("document", JSON_DOCUMENT.getBytes())).andDo(print()).andExpect(status().isNotFound()).andReturn();
  }

  @Test
  public void testValidateKnownVersion() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate?version=1").file("document", JSON_DOCUMENT.getBytes())).andDo(print()).andExpect(status().isNoContent()).andReturn();
  }

  @Test
  public void testValidateUnknownSchemaId() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + INVALID_SCHEMA_ID + "/validate").file("document", JSON_DOCUMENT.getBytes())).andDo(print()).andExpect(status().isNotFound()).andReturn();
  }

  @Test
  public void testValidateWithInvalidDocument() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate").file("document", INVALID_JSON_DOCUMENT.getBytes())).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testValidateWithEmptyDocument() throws Exception {
    ingestSchemaRecord();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate").file("document", "".getBytes())).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testValidateWithoutDocument() throws Exception {
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate")).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testValidateWithoutValidator() throws Exception {
    ingestSchemaRecord();

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate").file("document", DC_DOCUMENT.getBytes())).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testValidateWithMissingSchemaFile() throws Exception {
    ingestSchemaRecord();
    // Get location of schema file.
    String contentUri = contentInformationDao.findAll(PageRequest.of(0, 2)).getContent().get(0).getContentUri();
    //delete schema file
    URI uri = new URI(contentUri);
    Files.delete(Paths.get(uri));

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json/validate").file("document", JSON_DOCUMENT.getBytes())).andDo(print()).andExpect(status().isInternalServerError()).andReturn();
  }

  @Test
  public void testUpdateRecord() throws Exception {
    String schemaId = "updateRecord4Json".toLowerCase(Locale.getDefault());
    final String newLabel = "new label";
    final String newTechnicalInfo = "";
    final String newAbstract = "new abstract";
    final String newMethods = "new methods";

    ingestSchemaRecord(schemaId);
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    DataResource record = mapper.readValue(body, DataResource.class);
    String mimeTypeBefore = record.getFormats().iterator().next();
    String labelBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.OTHER).getDescription();
    String technicalInfoBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.TECHNICAL_INFO).getDescription();
    String abstractBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.ABSTRACT).getDescription();
    String methodsBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.METHODS).getDescription();
    record.getFormats().clear();
    record.getFormats().add(MediaType.APPLICATION_XML.toString());
    DataResourceRecordUtil.getDescription(record, Description.TYPE.TECHNICAL_INFO).setDescription(newTechnicalInfo);
    DataResourceRecordUtil.getDescription(record, Description.TYPE.OTHER).setDescription(newLabel);
    DataResourceRecordUtil.getDescription(record, Description.TYPE.ABSTRACT).setDescription(newAbstract);
    DataResourceRecordUtil.getDescription(record, Description.TYPE.METHODS).setDescription(newMethods);
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + schemaId).
            file(recordFile).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andExpect(redirectedUrlPattern("http://*:*/**/" + record.getId() + "?version=*")).andReturn();
    body = result.getResponse().getContentAsString();

    DataResource record2 = mapper.readValue(body, DataResource.class);
    Assert.assertEquals(record.getFormats().size(), record2.getFormats().size());
    Assert.assertNotEquals(mimeTypeBefore, record2.getFormats().iterator().next()); //mime type was changed
    Assert.assertEquals(DataResourceRecordUtil.getCreationDate(record), DataResourceRecordUtil.getCreationDate(record2));
    // Version shouldn't be updated
    Assert.assertEquals(record.getId(), record2.getId());
    Assert.assertEquals(record.getVersion(), record2.getVersion());//version is not changing for metadata update
    if (record.getAcls() != null) {
      Assert.assertTrue(record.getAcls().containsAll(record2.getAcls()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
    Assert.assertEquals("Check label: ", DataResourceRecordUtil.getDescription(record, Description.TYPE.OTHER), DataResourceRecordUtil.getDescription(record2, Description.TYPE.OTHER));
    Assert.assertEquals("Check abstract: ", DataResourceRecordUtil.getDescription(record, Description.TYPE.ABSTRACT), DataResourceRecordUtil.getDescription(record2, Description.TYPE.ABSTRACT));
    Assert.assertEquals("Check methods: ", DataResourceRecordUtil.getDescription(record, Description.TYPE.METHODS), DataResourceRecordUtil.getDescription(record2, Description.TYPE.METHODS));
    Assert.assertNull("Check technical info: ", DataResourceRecordUtil.getDescription(record2, Description.TYPE.TECHNICAL_INFO).getDescription());
    Assert.assertNotEquals("Check label: ", labelBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.OTHER).getDescription());
    Assert.assertNotEquals("Check abstract: ", abstractBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.ABSTRACT).getDescription());
    Assert.assertNotEquals("Check methods: ", methodsBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.METHODS).getDescription());
    Assert.assertNotEquals("Check technical info: ", technicalInfoBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.TECHNICAL_INFO).getDescription());
  }

  @Test
  public void testUpdateRecordWithoutChanges() throws Exception {
    String schemaId = "updateRecordWithoutChanges4Json".toLowerCase(Locale.getDefault());
    ingestSchemaRecord(schemaId);
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    DataResource record = mapper.readValue(body, DataResource.class);
    String mimeTypeBefore = record.getFormats().iterator().next();
    record.getFormats().clear();
    record.getFormats().add(MediaType.APPLICATION_XML.toString());
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + schemaId).
            file(recordFile).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andExpect(redirectedUrlPattern("http://*:*/**/" + record.getId() + "?version=*")).andReturn();
    body = result.getResponse().getContentAsString();

    DataResource record2 = mapper.readValue(body, DataResource.class);
    Assert.assertNotEquals(mimeTypeBefore, record2.getFormats().iterator().next());//mime type was changed
    Assert.assertEquals(record.getFormats().iterator().next(), record2.getFormats().iterator().next());//mime type was changed
    Assert.assertEquals(DataResourceRecordUtil.getCreationDate(record), DataResourceRecordUtil.getCreationDate(record2));
    // Version shouldn't be updated
    Assert.assertEquals(record.getId(), record2.getId());
    Assert.assertEquals(record.getVersion(), record2.getVersion());
    if (record.getAcls() != null) {
      Assert.assertTrue(record.getAcls().containsAll(record2.getAcls()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
  }

  @Test
  public void testUpdateRecordACLonly() throws Exception {
    String schemaId = "updateRecordACLonly4Json".toLowerCase(Locale.getDefault());
    ingestSchemaRecord(schemaId);
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    DataResource record = mapper.readValue(body, DataResource.class);
    String mimeTypeBefore = record.getFormats().iterator().next();
    String labelBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.OTHER).getDescription();
    String technicalInfoBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.TECHNICAL_INFO).getDescription();
    String abstractBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.ABSTRACT).getDescription();
    String methodsBefore = DataResourceRecordUtil.getDescription(record, Description.TYPE.METHODS).getDescription();
    record.getAcls().add(new AclEntry("updateACL", PERMISSION.READ));

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + schemaId).
            file(recordFile).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andExpect(redirectedUrlPattern("http://*:*/**/" + record.getId() + "?version=*")).andReturn();
    body = result.getResponse().getContentAsString();

    DataResource record2 = mapper.readValue(body, DataResource.class);
    Assert.assertEquals(mimeTypeBefore, record2.getFormats().iterator().next());//mime type was changed by update
    Assert.assertEquals(DataResourceRecordUtil.getCreationDate(record), DataResourceRecordUtil.getCreationDate(record2));
    // Version shouldn't be updated
    Assert.assertEquals("Check label: ", labelBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.OTHER).getDescription());
    Assert.assertEquals("Check abstract: ", abstractBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.ABSTRACT).getDescription());
    Assert.assertEquals("Check methods: ", methodsBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.METHODS).getDescription());
    Assert.assertEquals("Check technical info: ", technicalInfoBefore, DataResourceRecordUtil.getDescription(record2, Description.TYPE.TECHNICAL_INFO).getDescription());
    if (record.getAcls() != null) {
      Assert.assertTrue(SchemaRegistryControllerTestV2.isSameSetOfAclEntries(record.getAcls(), record2.getAcls()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
  }

  @Test
  public void testUpdateRecordAndDocument() throws Exception {
    String schemaId = "updateRecordAndDocument4Json".toLowerCase(Locale.getDefault());
    ingestSchemaRecord(schemaId);
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    DataResource record = mapper.readValue(body, DataResource.class);
    String mimeTypeBefore = record.getFormats().iterator().next();
    record.getFormats().clear();
    record.getFormats().add(MediaType.APPLICATION_XML.toString());
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA_V2.getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + schemaId).
            file(recordFile).file(schemaFile).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andExpect(redirectedUrlPattern("http://*:*/**/" + record.getId() + "?version=*")).andReturn();
    body = result.getResponse().getContentAsString();

    DataResource record2 = mapper.readValue(body, DataResource.class);
    Assert.assertNotEquals(mimeTypeBefore, record2.getFormats().iterator().next());//mime type was changed
    Assert.assertEquals(record.getFormats().iterator().next(), record2.getFormats().iterator().next());//mime type was not changed (as it is linked to schema)
    Assert.assertEquals(DataResourceRecordUtil.getCreationDate(record), DataResourceRecordUtil.getCreationDate(record2));
    testForNextVersion(record.getVersion(), record2.getVersion());
    Assert.assertEquals(record.getId(), record2.getId());
    if (record.getAcls() != null) {
      Assert.assertTrue(record.getAcls().containsAll(record2.getAcls()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
    // Test also document for update
    result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId)).andDo(print()).andExpect(status().isOk()).andReturn();
    String content = result.getResponse().getContentAsString();

    Assert.assertEquals(JSON_SCHEMA_V2, content);
  }

  @Test
  public void testUpdateOnlyDocument() throws Exception {
    String schemaId = "updateRecordDocumentOnly4Json".toLowerCase(Locale.getDefault());
    ingestSchemaRecord(schemaId);
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    DataResource record = mapper.readValue(body, DataResource.class);
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA_V2.getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + schemaId).
            file(schemaFile).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andExpect(redirectedUrlPattern("http://*:*/**/" + record.getId() + "?version=*")).andReturn();
    body = result.getResponse().getContentAsString();

    DataResource record2 = mapper.readValue(body, DataResource.class);
    Assert.assertEquals(record.getFormats().iterator().next(), record2.getFormats().iterator().next());//mime type was changed by update
    Assert.assertEquals(DataResourceRecordUtil.getCreationDate(record), DataResourceRecordUtil.getCreationDate(record2));
    testForNextVersion(record.getVersion(), record2.getVersion());
    Assert.assertEquals(record.getId(), record2.getId());
    if (record.getAcls() != null) {
      Assert.assertTrue(record.getAcls().containsAll(record2.getAcls()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
    // Test also document for update
    result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId)).andDo(print()).andExpect(status().isOk()).andReturn();
    String content = result.getResponse().getContentAsString();

    Assert.assertEquals(JSON_SCHEMA_V2, content);
  }

  @Test
  public void testUpdateRecordWithoutExplizitGet() throws Exception {
    String schemaId = "updateWithoutGet4Json".toLowerCase(Locale.getDefault());
    DataResource record = new DataResource();
    record.setId(schemaId);
    record.getTitles().add(Title.factoryTitle(schemaId));
    record.getFormats().add(MediaType.APPLICATION_JSON.toString());
    record.setResourceType(ResourceType.createResourceType(DataResourceRecordUtil.JSON_SCHEMA_TYPE, ResourceType.TYPE_GENERAL.MODEL));
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcls(aclEntries);

    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());

    MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    mapper = new ObjectMapper();
    DataResource record1 = mapper.readValue(body, DataResource.class);
    String mimeTypeBefore = record1.getFormats().iterator().next();
    record1.getFormats().clear();
    record1.getFormats().add(MediaType.APPLICATION_XML.toString());
    recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record1).getBytes());
    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + schemaId).
            file(recordFile).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andReturn();
    body = result.getResponse().getContentAsString();

    DataResource record2 = mapper.readValue(body, DataResource.class);
    Assert.assertNotEquals(mimeTypeBefore, record2.getFormats().iterator().next());//mime type was changed
    Assert.assertEquals(record1.getFormats().iterator().next(), record2.getFormats().iterator().next());//mime type was not changed (as it is linked to schema)
    Assert.assertEquals(DataResourceRecordUtil.getCreationDate(record1), DataResourceRecordUtil.getCreationDate(record2));
    // Version shouldn't be updated
    Assert.assertEquals(record1.getId(), record2.getId());
    Assert.assertEquals(record1.getVersion(), record2.getVersion());
    if (record1.getAcls() != null) {
      Assert.assertTrue(record1.getAcls().containsAll(record2.getAcls()));
    }
    Assert.assertTrue(record1.getLastUpdate().isBefore(record2.getLastUpdate()));
  }

  @Test
  public void testUpdateRecordWithoutETag() throws Exception {
    ingestSchemaRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/json").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String body = result.getResponse().getContentAsString();
    ObjectMapper mapper = new ObjectMapper();
    DataResource record = mapper.readValue(body, DataResource.class);
    record.getFormats().add(MediaType.APPLICATION_JSON.toString());

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json").
            file(recordFile).with(putMultipart())).andDo(print()).andExpect(status().isPreconditionRequired()).andReturn();
  }

  @Test
  public void testUpdateRecordWithWrongETag() throws Exception {
    ingestSchemaRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/json").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag") + "unknown";
    String body = result.getResponse().getContentAsString();
    ObjectMapper mapper = new ObjectMapper();
    DataResource record = mapper.readValue(body, DataResource.class);
    record.getDescriptions().add(Description.factoryDescription("any", Description.TYPE.OTHER));
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/json").
            file(recordFile).with(putMultipart())).andDo(print()).andExpect(status().isPreconditionRequired()).andReturn();
  }

  @Test
  public void testUpdateRecordWithoutBody() throws Exception {
    ingestSchemaRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/json").header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");

    this.mockMvc.perform(put("/api/v2/schemas/json").header("If-Match", etag).contentType(DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE).content("{}")).andDo(print()).andExpect(status().isUnsupportedMediaType()).andReturn();
  }

  @Test
  public void testCreateSchemaRecordWithUpdateWithoutChanges() throws Exception {
    String schemaId = "updateWithoutChanges_json";
    // Test with a schema missing schema property.
    DataResource record = new DataResource();
    record.setId(schemaId.toLowerCase(Locale.getDefault()));
    record.getTitles().add(Title.factoryTitle(schemaId));
    record.setResourceType(ResourceType.createResourceType(DataResourceRecordUtil.JSON_SCHEMA_TYPE, ResourceType.TYPE_GENERAL.MODEL));
    record.getFormats().add(MediaType.APPLICATION_JSON.toString());
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcls(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA4UPDATE.getBytes());

    MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    DataResource record1 = mapper.readValue(body, DataResource.class);
    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/" + record.getId()).
            file(schemaFile).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andExpect(redirectedUrlPattern("http://*:*/**/" + record.getId() + "?version=*")).andReturn();
    body = result.getResponse().getContentAsString();

    DataResource record2 = mapper.readValue(body, DataResource.class);
    Assert.assertEquals(record1.getFormats().iterator().next(), record2.getFormats().iterator().next());//mime type was changed by update
    Assert.assertEquals(DataResourceRecordUtil.getCreationDate(record1), DataResourceRecordUtil.getCreationDate(record2));
    // Version shouldn't be updated
    Assert.assertEquals(record1.getId(), record2.getId());
    Assert.assertEquals(record1.getVersion(), record2.getVersion());//version is not changing for metadata update
    if (record1.getAcls() != null) {
      Assert.assertTrue(record1.getAcls().containsAll(record2.getAcls()));
    }
    Assert.assertTrue(record1.getLastUpdate().isBefore(record2.getLastUpdate()));
  }

  @Test
  public void testDeleteSchemaRecord() throws Exception {
    String schemaId = "testDeleteJson".toLowerCase(Locale.getDefault());
    ingestSchemaRecord(schemaId);

    MvcResult result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");

    this.mockMvc.perform(delete("/api/v2/schemas/" + schemaId).header("If-Match", etag)).andDo(print()).andExpect(status().isNoContent()).andReturn();
    // create should return conflict
    DataResource schemaRecord = DataResource.factoryNewDataResource(schemaId);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(schemaRecord).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.json", "application/json", JSON_SCHEMA.getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isConflict()).andReturn();
    //delete second time // should be really deleted -> gone
    result = this.mockMvc.perform(get("/api/v2/schemas/" + schemaId).header("Accept", DataResourceRecordUtil.DATA_RESOURCE_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    etag = result.getResponse().getHeader("ETag");
    this.mockMvc.perform(delete("/api/v2/schemas/" + schemaId).header("If-Match", etag)).andDo(print()).andExpect(status().isNoContent()).andReturn();

    //try to create after deletion (Should return HTTP GONE)
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v2/schemas/").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isGone()).andReturn();
  }

  private void ingestSchemaRecord() throws Exception {
    ingestSchemaRecord("json");
  }

  private void ingestSchemaRecord(String schemaId) throws Exception {
    DataResource dataResource = DataResource.factoryNewDataResource(schemaId);
    dataResource.getCreators().add(Agent.factoryAgent(null, "SELF"));
    dataResource.getTitles().add(Title.factoryTitle(schemaId, Title.TYPE.OTHER));
    dataResource.setPublisher("SELF");
    Instant now = Instant.now();
    dataResource.setPublicationYear(Integer.toString(Calendar.getInstance().get(Calendar.YEAR)));
    dataResource.setResourceType(ResourceType.createResourceType(DataResourceRecordUtil.JSON_SCHEMA_TYPE, ResourceType.TYPE_GENERAL.MODEL));
    dataResource.getDates().add(Date.factoryDate(now, Date.DATE_TYPE.CREATED));
    dataResource.getFormats().add(MediaType.APPLICATION_JSON.toString());
    dataResource.setLastUpdate(now);
    dataResource.setState(DataResource.State.VOLATILE);
    dataResource.setVersion("1");
    Set<AclEntry> aclEntries = dataResource.getAcls();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    Set<Description> descriptions = dataResource.getDescriptions();
    descriptions.add(Description.factoryDescription("other", Description.TYPE.OTHER));
    descriptions.add(Description.factoryDescription("abstract", Description.TYPE.ABSTRACT));
    descriptions.add(Description.factoryDescription("technical info", Description.TYPE.TECHNICAL_INFO));
    descriptions.add(Description.factoryDescription("not used yet", Description.TYPE.METHODS));
    ContentInformation ci = ContentInformation.createContentInformation(
            "json", "schema.json", (String[]) null);
    ci.setVersion(1);
    ci.setFileVersion("1");
    ci.setVersioningService("simple");
    ci.setDepth(1);
    ci.setContentUri("file:/tmp/json.json");
    ci.setUploader("SELF");
    ci.setMediaType("text/plain");
    ci.setHash("sha1:400dfe162fd702a619c4d11ddfb3b7550cb9dec7");
    ci.setSize(1097);

    dataResource = schemaConfig.getDataResourceService().create(dataResource, "SELF");
    ci.setParentResource(dataResource);

    contentInformationDao.save(ci);
    schemaConfig.getContentInformationAuditService().captureAuditInformation(ci, "SELF");

    SchemaRecord schemaRecord = new SchemaRecord();
    schemaRecord.setSchemaId(dataResource.getId() + "/1");
    schemaRecord.setVersion(1L);
    schemaRecord.setType(MetadataSchemaRecord.SCHEMA_TYPE.JSON);
    schemaRecord.setSchemaDocumentUri(ci.getContentUri());
    schemaRecord.setDocumentHash(ci.getHash());
    schemaRecordDao.save(schemaRecord);

    File jsonFile = new File("/tmp/json.json");
    if (!jsonFile.exists()) {
      try (FileOutputStream fout = new FileOutputStream(jsonFile)) {
        fout.write(JSON_SCHEMA.getBytes());
        fout.flush();
      }
    }
  }

  private static RequestPostProcessor remoteAddr(final String remoteAddr) { // it's nice to extract into a helper
    return (MockHttpServletRequest request) -> {
      request.setRemoteAddr(remoteAddr);
      return request;
    };
  }

  private static RequestPostProcessor putMultipart() { // it's nice to extract into a helper
    return (MockHttpServletRequest request) -> {
      request.setMethod("PUT");
      return request;
    };
  }

  private void testForNextVersion(String first, String second) {
    int index = first.lastIndexOf("=");
    int firstVersion = Integer.parseInt(first.substring(index + 1));
    int secondVersion = Integer.parseInt(second.substring(index + 1));
    Assert.assertEquals(firstVersion + 1, secondVersion);
    if (index > 0) {
      Assert.assertEquals(first.substring(0, index), second.substring(0, index));
    }
  }
}
