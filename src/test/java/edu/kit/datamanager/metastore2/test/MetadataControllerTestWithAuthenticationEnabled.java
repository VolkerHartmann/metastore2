/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.metastore2.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.kit.datamanager.entities.PERMISSION;
import edu.kit.datamanager.entities.RepoUserRole;
import edu.kit.datamanager.metastore2.configuration.ApplicationProperties;
import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.dao.IDataRecordDao;
import edu.kit.datamanager.metastore2.dao.ILinkedMetadataRecordDao;
import edu.kit.datamanager.metastore2.dao.ISchemaRecordDao;
import edu.kit.datamanager.metastore2.dao.IUrl2PathDao;
import edu.kit.datamanager.metastore2.domain.MetadataRecord;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import edu.kit.datamanager.metastore2.domain.ResourceIdentifier;
import edu.kit.datamanager.metastore2.domain.SchemaRecord;
import edu.kit.datamanager.repo.dao.IAllIdentifiersDao;
import edu.kit.datamanager.repo.dao.IContentInformationDao;
import edu.kit.datamanager.repo.dao.IDataResourceDao;
import edu.kit.datamanager.repo.domain.Agent;
import edu.kit.datamanager.repo.domain.ContentInformation;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.repo.domain.Date;
import edu.kit.datamanager.repo.domain.ResourceType;
import edu.kit.datamanager.repo.domain.Title;
import edu.kit.datamanager.repo.domain.acl.AclEntry;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.javers.core.Javers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.restdocs.JUnitRestDocumentation;
import static org.springframework.restdocs.mockmvc.MockMvcRestDocumentation.documentationConfiguration;
import org.springframework.security.test.context.support.WithSecurityContextTestExecutionListener;
import org.springframework.security.web.FilterChainProxy;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 *
 * @author Torridity
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT) //RANDOM_PORT)
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
@TestPropertySource(properties = {"server.port=41408"})
@TestPropertySource(properties = {"spring.datasource.url=jdbc:h2:mem:db_md_aai;DB_CLOSE_DELAY=-1"})
@TestPropertySource(properties = {"server.auth.enabled=true"})
@TestPropertySource(properties = {"metastore.metadata.schemaRegistries="})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class MetadataControllerTestWithAuthenticationEnabled {

  private final static String TEMP_DIR_4_ALL = "/tmp/metastore2/";
  private final static String TEMP_DIR_4_SCHEMAS = TEMP_DIR_4_ALL + "schema/";
  private final static String TEMP_DIR_4_METADATA = TEMP_DIR_4_ALL + "metadata/";
  private static final String METADATA_RECORD_ID = "test_id";
  private static final String SCHEMA_ID = "my_dc";
  private static final String INVALID_SCHEMA = "invalid_dc";
  private static final ResourceIdentifier RELATED_RESOURCE = ResourceIdentifier.factoryUrlResourceIdentifier("anyResourceId");
  private static final ResourceIdentifier RELATED_RESOURCE_2 = ResourceIdentifier.factoryUrlResourceIdentifier("anyOtherResourceId");
  private final static String DC_SCHEMA = "<schema targetNamespace=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"\n"
          + "        xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"\n"
          + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
          + "        xmlns=\"http://www.w3.org/2001/XMLSchema\"\n"
          + "        elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
          + "\n"
          + "<import namespace=\"http://purl.org/dc/elements/1.1/\" schemaLocation=\"https://www.dublincore.org/schemas/xmls/qdc/2008/02/11/dc.xsd\"/>\n"
          + "\n"
          + "<element name=\"dc\" type=\"oai_dc:oai_dcType\"/>\n"
          + "\n"
          + "<complexType name=\"oai_dcType\">\n"
          + "  <choice minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
          + "    <element ref=\"dc:title\"/>\n"
          + "    <element ref=\"dc:creator\"/>\n"
          + "    <element ref=\"dc:subject\"/>\n"
          + "    <element ref=\"dc:description\"/>\n"
          + "    <element ref=\"dc:publisher\"/>\n"
          + "    <element ref=\"dc:contributor\"/>\n"
          + "    <element ref=\"dc:date\"/>\n"
          + "    <element ref=\"dc:type\"/>\n"
          + "    <element ref=\"dc:format\"/>\n"
          + "    <element ref=\"dc:identifier\"/>\n"
          + "    <element ref=\"dc:source\"/>\n"
          + "    <element ref=\"dc:language\"/>\n"
          + "    <element ref=\"dc:relation\"/>\n"
          + "    <element ref=\"dc:coverage\"/>\n"
          + "    <element ref=\"dc:rights\"/>\n"
          + "  </choice>\n"
          + "</complexType>\n"
          + "\n"
          + "</schema>";

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
  private final static String DC_DOCUMENT_VERSION_2 = "<?xml version='1.0' encoding='utf-8'?>\n"
          + "<oai_dc:dc xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">\n"
          + "  <dc:creator>Else, Someone</dc:creator>\n"
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
  private final static String DC_DOCUMENT_WRONG_NAMESPACE = "<?xml version='1.0' encoding='utf-8'?>\n"
          + "<oai_dc:dc xmlns:dc=\"http://purl.org/dc/elements/NOT_EXIST/\" xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">\n"
          + "  <dc:creator>Mungall, Chris</dc:creator>\n"
          + "  <dc:date>2018-07-02</dc:date>\n"
          + "  <dc:description>Archival bundle of GO data release.</dc:description>\n"
          + "  <dc:identifier>oai:zenodo.org:3477535</dc:identifier>\n"
          + "  <dc:relation>url:https://zenodo.org/communities/zenodo</dc:relation>\n"
          + "  <dc:rights>http://creativecommons.org/licenses/by/4.0/legalcode</dc:rights>\n"
          + "  <dc:title>Gene Ontology Data Archive</dc:title>\n"
          + "  <dc:type>dataset</dc:type>\n"
          + "</oai_dc:dc>";
  private final static String DC_DOCUMENT_INVALID = "<?xml version='1.0' encoding='utf-8'?>\n"
          + "<oai_dc:dc xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">\n"
          + "  <dc:creater>Carbon, Seth</dc:creater>\n"
          + "</oai_dc:dc>";

 
  private String adminToken;
  private String userToken;
  private String otherUserToken;
  private String guestToken;

  private DataResource sampleResource;
 private static Boolean alreadyInitialized = Boolean.FALSE;

  private MockMvc mockMvc;
  @Autowired
  private ApplicationProperties applicationProperties;
  @Autowired
  private WebApplicationContext context;
  @Autowired
  private FilterChainProxy springSecurityFilterChain;
  @Autowired
  Javers javers = null;
  @Autowired
  private ILinkedMetadataRecordDao metadataRecordDao;
  @Autowired
  private IDataResourceDao dataResourceDao;
 @Autowired
  private IDataRecordDao dataRecordDao;
  @Autowired
  private ISchemaRecordDao schemaRecordDao;
  @Autowired
  private IContentInformationDao contentInformationDao;
  @Autowired
  private IAllIdentifiersDao allIdentifiersDao;
  @Autowired
  private IUrl2PathDao url2PathDao;
  @Autowired
  private MetastoreConfiguration metadataConfig;
  @Rule
  public JUnitRestDocumentation restDocumentation = new JUnitRestDocumentation();

  @Before
  public void setUp() throws Exception {
    System.out.println("------MetadataControllerTest--------------------------");
    System.out.println("------" + this.metadataConfig);
    System.out.println("------------------------------------------------------");

    contentInformationDao.deleteAll();
    dataResourceDao.deleteAll();
    metadataRecordDao.deleteAll();
    schemaRecordDao.deleteAll();
    dataRecordDao.deleteAll();
    allIdentifiersDao.deleteAll();
    url2PathDao.deleteAll();
    
    try {
      // setup mockMvc
      this.mockMvc = MockMvcBuilders.webAppContextSetup(this.context)
              .addFilters(springSecurityFilterChain)
              .apply(documentationConfiguration(this.restDocumentation).uris()
      				.withPort(41408))
              .build();
      // Create schema only once.
      try (Stream<Path> walk = Files.walk(Paths.get(URI.create("file://" + TEMP_DIR_4_SCHEMAS)))) {
        walk.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
      }
      Paths.get(TEMP_DIR_4_SCHEMAS).toFile().mkdir();
      Paths.get(TEMP_DIR_4_SCHEMAS + INVALID_SCHEMA).toFile().createNewFile();
      ingestSchemaRecord();
      try (Stream<Path> walk = Files.walk(Paths.get(URI.create("file://" + TEMP_DIR_4_METADATA)))) {
        walk.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
      }
      Paths.get(TEMP_DIR_4_METADATA).toFile().mkdir();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
 
    adminToken = edu.kit.datamanager.util.JwtBuilder.createUserToken("admin", RepoUserRole.ADMINISTRATOR).
            addSimpleClaim("email", "thomas.jejkal@kit.edu").
            addSimpleClaim("orcid", "0000-0003-2804-688X").
            addSimpleClaim("groupid", "USERS").
            addSimpleClaim("loginFailures", 0).
            addSimpleClaim("active", true).
            addSimpleClaim("locked", false).
            getCompactToken(applicationProperties.getJwtSecret());

    userToken = edu.kit.datamanager.util.JwtBuilder.createUserToken("user", RepoUserRole.USER).
            addSimpleClaim("email", "thomas.jejkal@kit.edu").
            addSimpleClaim("orcid", "0000-0003-2804-688X").
            addSimpleClaim("loginFailures", 0).
            addSimpleClaim("active", true).
            addSimpleClaim("locked", false).
            getCompactToken(applicationProperties.getJwtSecret());

    otherUserToken = edu.kit.datamanager.util.JwtBuilder.createUserToken("otheruser", RepoUserRole.USER).
            addSimpleClaim("email", "thomas.jejkal@kit.edu").
            addSimpleClaim("orcid", "0000-0003-2804-688X").
            addSimpleClaim("loginFailures", 0).
            addSimpleClaim("active", true).
            addSimpleClaim("locked", false).getCompactToken(applicationProperties.getJwtSecret());

    guestToken = edu.kit.datamanager.util.JwtBuilder.createUserToken("guest", RepoUserRole.GUEST).
            addSimpleClaim("email", "thomas.jejkal@kit.edu").
            addSimpleClaim("orcid", "0000-0003-2804-688X").
            addSimpleClaim("loginFailures", 0).
            addSimpleClaim("active", true).
            addSimpleClaim("locked", false).getCompactToken(applicationProperties.getJwtSecret());
 }

  @Test
  public void testCreateRecord() throws Exception {
    MetadataRecord record = new MetadataRecord();
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
//    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
//    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
//    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isCreated()).andExpect(redirectedUrlPattern("http://*:*/**/*?version=1")).andReturn();
  }

  @Test
  public void testCreateRecordWithValidUrlSchema() throws Exception {
    MetadataRecord record = new MetadataRecord();
    // Get URL of schema
    String schemaUrl = getSchemaUrl(SCHEMA_ID);
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryUrlResourceIdentifier(schemaUrl));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
//    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
//    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
//    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile)).andDo(print()).andExpect(status().isCreated()).andExpect(redirectedUrlPattern("http://*:*/**/*?version=1")).andReturn();
  }

  @Test
  public void testCreateRecordWithUrlSchemaNull() throws Exception {
    MetadataRecord record = new MetadataRecord();

    record.setSchema(ResourceIdentifier.factoryUrlResourceIdentifier(null));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
//    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
//    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
//    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile)).
            andDo(print()).
            andExpect(status().isBadRequest()).
            andReturn();
  }

  @Test
  public void testCreateRecordWithInvalidUrl() throws Exception {
    MetadataRecord record = new MetadataRecord();
    // Get URL of schema and remove first character
    String invalidSchemaUrl = getSchemaUrl(SCHEMA_ID).substring(1);
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryUrlResourceIdentifier(invalidSchemaUrl));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
//    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
//    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
//    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile)).
            andDo(print()).
            andExpect(status().isUnprocessableEntity()).
            andReturn();
  }

  @Test
  public void testCreateRecordWithInvalidUrlSchema() throws Exception {
    MetadataRecord record = new MetadataRecord();
    // Get URL of schema
    String urlWithInvalidSchema = getSchemaUrl(SCHEMA_ID).replace(SCHEMA_ID, INVALID_SCHEMA);
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryUrlResourceIdentifier(urlWithInvalidSchema));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
//    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
//    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
//    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile)).
            andDo(print()).
            andExpect(status().isUnprocessableEntity()).
            andReturn();
  }

  @Test
  public void testCreateRecordWithAnyValidUrl() throws Exception {
    MetadataRecord record = new MetadataRecord();
    // Get URL of schema
    String schemaUrl = "http://anyurl.example.org/shouldNotExist";
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryUrlResourceIdentifier(schemaUrl));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
//    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
//    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
//    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile)).
            andDo(print()).
            andExpect(status().isUnprocessableEntity()).
            andReturn();
  }

  @Test
  public void testCreateRecordWithId() throws Exception {
    MetadataRecord record = new MetadataRecord();
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    record.setId("SomeInvalidId");
    Set<AclEntry> aclEntries = new HashSet<>();
//    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
//    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
//    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateRecordWithLocationUri() throws Exception {
    MetadataRecord record = new MetadataRecord();
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("SELF", PERMISSION.READ));
    aclEntries.add(new AclEntry("test2", PERMISSION.ADMINISTRATE));
    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isCreated()).andExpect(redirectedUrlPattern("http://*:*/**/*?version=1")).andReturn();
    String locationUri = result.getResponse().getHeader("Location");
    String content = result.getResponse().getContentAsString();

    ObjectMapper map = new ObjectMapper();
    MvcResult result2 = this.mockMvc.perform(get(locationUri).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String content2 = result2.getResponse().getContentAsString();

    Assert.assertEquals(content, content2);
  }

  @Test
  public void testCreateInvalidRecord() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(INVALID_SCHEMA));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testCreateInvalidMetadataRecord() throws Exception {
    String wrongTypeJson = "{\"id\":\"dc\",\"relatedResource\":\"anyResource\",\"createdAt\":\"right now!\"}";

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", wrongTypeJson.getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(schemaFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
    String wrongFormatJson = "<metadata><schemaId>dc</schemaId><type>XML</type></metadata>";
    recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", wrongFormatJson.getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(schemaFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();

  }

  @Test
  public void testCreateEmptyMetadataSchemaRecord() throws Exception {

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", (byte[]) null);
    MockMultipartFile schemaFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(schemaFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();

    recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", " ".getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(schemaFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  // @Test 
  public void testCreateRecordFromExternal() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());
    RequestPostProcessor rpp = new RequestPostProcessor() {
      @Override
      public MockHttpServletRequest postProcessRequest(MockHttpServletRequest mhsr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).with(remoteAddr("any.external.domain"))).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  //@Test @ToDo Set external remote address.
  public void testCreateRecordUpdateFromExternal() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier("my_dcExt"));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).with(remoteAddr("any.domain.com"))).andDo(print()).andExpect(status().isCreated()).andReturn();
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).with(remoteAddr("www.google.com"))).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  @Test
  public void testCreateMetadataUnknownSchemaId() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier("unknown_dc"));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    MvcResult res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testCreateRecordWithBadMetadata() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", "<>".getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testCreateRecordWithInvalidMetadataNamespace() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_WRONG_NAMESPACE.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testCreateRecordWithInvalidMetadata() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_INVALID.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isUnprocessableEntity()).andReturn();
  }

  @Test
  public void testCreateRecordWithoutRecord() throws Exception {
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateRecordWithoutSchema() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateRecordWithBadRecord() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    MetadataRecord record = new MetadataRecord();
    //schemaId is missing
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(null));
    record.setRelatedResource(RELATED_RESOURCE);

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateRecordWithBadRecord2() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    //related resource is missing

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isBadRequest()).andReturn();
  }

  @Test
  public void testCreateTwoVersionsOfSameRecord() throws Exception {
    MetadataRecord record = new MetadataRecord();
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    MvcResult res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isCreated()).andReturn();

    MetadataRecord result = mapper.readValue(res.getResponse().getContentAsString(), MetadataRecord.class);
    Assert.assertEquals(Long.valueOf(1l), result.getRecordVersion());

    res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isConflict()).andReturn();

    Assert.assertTrue(res.getResponse().getContentAsString().contains("Metadata record already exists"));
  }

  @Test
  public void testCreateTwoVersions() throws Exception {
    MetadataRecord record = new MetadataRecord();
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    MvcResult res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isCreated()).andReturn();

    MetadataRecord result = mapper.readValue(res.getResponse().getContentAsString(), MetadataRecord.class);
    Assert.assertEquals(Long.valueOf(1l), result.getRecordVersion());

    record.setRelatedResource(RELATED_RESOURCE_2);
    recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    res = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isCreated()).andReturn();

    result = mapper.readValue(res.getResponse().getContentAsString(), MetadataRecord.class);
    Assert.assertEquals(Long.valueOf(1l), result.getRecordVersion());
  }

  @Test
  public void testGetRecordById() throws Exception {
    String metadataRecordId = createDCMetadataRecord();

    MvcResult res = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    MetadataRecord result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord.class);
    Assert.assertNotNull(result);
    Assert.assertEquals(ResourceIdentifier.IdentifierType.URL, result.getSchema().getIdentifierType());
    String schemaUrl = result.getSchema().getIdentifier();
    Assert.assertTrue(schemaUrl.startsWith("http://localhost:"));
    Assert.assertTrue(schemaUrl.contains("/api/v1/schemas/"));
    Assert.assertTrue(schemaUrl.contains(SCHEMA_ID));
    //Schema URI must not be the actual file URI but the link to the REST endpoint for downloading the schema
    Assert.assertNotEquals("file:///tmp/dc.xml", result.getMetadataDocumentUri());
  }

  @Test
  public void testGetRecordByIdWithVersion() throws Exception {
    String metadataRecordId = createDCMetadataRecord();

    MvcResult res = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).param("version", "1").header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    MetadataRecord result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord.class);
    Assert.assertNotNull(result);
    Assert.assertEquals(ResourceIdentifier.IdentifierType.URL, result.getSchema().getIdentifierType());
    String schemaUrl = result.getSchema().getIdentifier();
    Assert.assertTrue(schemaUrl.startsWith("http://localhost:"));
    Assert.assertTrue(schemaUrl.contains("/api/v1/schemas/"));
    Assert.assertTrue(schemaUrl.contains(SCHEMA_ID));
    Assert.assertNotEquals("file:///tmp/dc.xml", result.getMetadataDocumentUri());
  }

  @Test
  public void testGetRecordByIdWithInvalidId() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    this.mockMvc.perform(get("/api/v1/metadata/cd").header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isNotFound()).andReturn();
  }

  @Test
  public void testGetRecordByIdWithInvalidVersion() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).param("version", "13").header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().is4xxClientError()).andReturn();
  }

  @Test
  public void testFindRecordsBySchemaId() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult res = this.mockMvc.perform(get("/api/v1/metadata").param("schemaId", SCHEMA_ID).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    MetadataRecord[] result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord[].class);

    Assert.assertEquals(1, result.length);
  }

  @Test
  public void testFindRecordsByResourceId() throws Exception {
    Instant oneHourBefore = Instant.now().minusSeconds(3600);
    Instant twoHoursBefore = Instant.now().minusSeconds(7200);
    String metadataRecordId = createDCMetadataRecord();
    MvcResult res = this.mockMvc.perform(get("/api/v1/metadata").param("resoureId", RELATED_RESOURCE.getIdentifier()).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    MetadataRecord[] result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord[].class);

    Assert.assertEquals(1, result.length);
    res = this.mockMvc.perform(get("/api/v1/metadata").param("resourceId", RELATED_RESOURCE.getIdentifier()).param("from", twoHoursBefore.toString()).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    map = new ObjectMapper();
    result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord[].class);

    Assert.assertEquals(1, result.length);
  }

  @Test
  public void testFindRecordsByInvalidResourceId() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult res = this.mockMvc.perform(get("/api/v1/metadata").param("resourceId", "invalid").header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    MetadataRecord[] result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord[].class);

    Assert.assertEquals(0, result.length);
  }

  @Test
  public void testFindRecordsByInvalidUploadDate() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    Instant oneHourBefore = Instant.now().minusSeconds(3600);
    Instant twoHoursBefore = Instant.now().minusSeconds(7200);

    MvcResult res = this.mockMvc.perform(get("/api/v1/metadata").param("resourceId", RELATED_RESOURCE.getIdentifier()).param("until", oneHourBefore.toString()).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    MetadataRecord[] result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord[].class);

    Assert.assertEquals(0, result.length);

    res = this.mockMvc.perform(get("/api/v1/metadata").param("resourceId", RELATED_RESOURCE.getIdentifier()).param("from", twoHoursBefore.toString()).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).param("until", oneHourBefore.toString())).andDo(print()).andExpect(status().isOk()).andReturn();
    map = new ObjectMapper();
    result = map.readValue(res.getResponse().getContentAsString(), MetadataRecord[].class);

    Assert.assertEquals(0, result.length);
  }

  @Test
  public void testFindRecordsByUnknownParameter() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    this.mockMvc.perform(get("/api/v1/metadata").param("schemaId", "cd").header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(0)));
  }

  @Test
  public void testGetSchemaDocument() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    String content = result.getResponse().getContentAsString();

    String dcMetadata = DC_DOCUMENT;

    Assert.assertEquals(dcMetadata, content);
  }

  @Test
  public void testGetMetadataDocumentWithUnknownSchema() throws Exception {
    String metadataRecordId = createDCMetadataRecord();

    this.mockMvc.perform(get("/api/v1/metadata/unknown_dc").header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isNotFound()).andReturn();
  }

  @Test
  public void testUpdateRecord() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    MetadataRecord record = mapper.readValue(body, MetadataRecord.class);
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_VERSION_2.getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata/" + record.getId()).
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andReturn();

//    result = this.mockMvc.perform(put("/api/v1/metadata/dc").header("If-Match", etag).contentType(MetadataRecord.METADATA_RECORD_MEDIA_TYPE).content(mapper.writeValueAsString(record))).andDo(print()).andExpect(status().isOk()).andReturn();
    body = result.getResponse().getContentAsString();

    MetadataRecord record2 = mapper.readValue(body, MetadataRecord.class);
    Assert.assertNotEquals(record.getDocumentHash(), record2.getDocumentHash());//mime type was changed by update
    Assert.assertEquals(record.getCreatedAt(), record2.getCreatedAt());
    Assert.assertEquals(record.getSchema().getIdentifier(), record2.getSchema().getIdentifier());
    Assert.assertEquals((long) record.getRecordVersion(), record2.getRecordVersion() - 1l);// version should be 1 higher
    if (record.getAcl() != null) {
      Assert.assertTrue(record.getAcl().containsAll(record2.getAcl()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
    // Check for new metadata document.
    result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    String content = result.getResponse().getContentAsString();

    String dcMetadata = DC_DOCUMENT_VERSION_2;

    Assert.assertEquals(dcMetadata, content);

    Assert.assertEquals(record.getMetadataDocumentUri().replace("version=1", "version=2"), record2.getMetadataDocumentUri());
  }

  @Test
  public void testUpdateRecordWithoutExplizitGet() throws Exception {
    MetadataRecord record = new MetadataRecord();
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> acl = new HashSet<>();
    acl.add(new AclEntry("test", PERMISSION.READ));
    acl.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcl(acl);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isCreated()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();
    String locationUri = result.getResponse().getHeader("Location");

    MetadataRecord record2 = mapper.readValue(body, MetadataRecord.class);
    MockMultipartFile recordFile2 = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record2).getBytes());
    MockMultipartFile metadataFile2 = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_VERSION_2.getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart(locationUri).
            file(recordFile2).
            file(metadataFile2).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andReturn();

//    result = this.mockMvc.perform(put("/api/v1/metadata/dc").header("If-Match", etag).contentType(MetadataRecord.METADATA_RECORD_MEDIA_TYPE).content(mapper.writeValueAsString(record))).andDo(print()).andExpect(status().isOk()).andReturn();
    body = result.getResponse().getContentAsString();

    MetadataRecord record3 = mapper.readValue(body, MetadataRecord.class);
    Assert.assertNotEquals(record2.getDocumentHash(), record3.getDocumentHash());//mime type was changed by update
    Assert.assertEquals(record2.getCreatedAt(), record3.getCreatedAt());
    Assert.assertEquals(record2.getMetadataDocumentUri().replace("version=1", "version=2"), record3.getMetadataDocumentUri());
    Assert.assertEquals(record2.getSchema().getIdentifier(), record3.getSchema().getIdentifier());
    Assert.assertEquals((long) record2.getRecordVersion(), record3.getRecordVersion() - 1l);// version should be 1 higher
    if (record2.getAcl() != null) {
      Assert.assertTrue(record2.getAcl().containsAll(record3.getAcl()));
    }
    Assert.assertTrue(record2.getLastUpdate().isBefore(record3.getLastUpdate()));
  }

  @Test
  public void testUpdateRecordWithoutETag() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    MetadataRecord record = mapper.readValue(body, MetadataRecord.class);
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_VERSION_2.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata/" + metadataRecordId).
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).with(putMultipart())).andDo(print()).andExpect(status().isPreconditionRequired()).andReturn();
  }

  @Test
  public void testUpdateRecordWithWrongETag() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag") + "unknown";
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    MetadataRecord record = mapper.readValue(body, MetadataRecord.class);
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_VERSION_2.getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata/" + metadataRecordId).
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isPreconditionFailed()).andReturn();
  }

  @Test
  public void testUpdateRecordWithoutRecord() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    MetadataRecord record = mapper.readValue(body, MetadataRecord.class);
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_VERSION_2.getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata/" + metadataRecordId).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andReturn();
    body = result.getResponse().getContentAsString();

    MetadataRecord record2 = mapper.readValue(body, MetadataRecord.class);
    Assert.assertNotEquals(record.getDocumentHash(), record2.getDocumentHash());//mime type was changed by update
    Assert.assertEquals(record.getCreatedAt(), record2.getCreatedAt());
    Assert.assertEquals(record.getMetadataDocumentUri().replace("version=1", "version=2"), record2.getMetadataDocumentUri());
    Assert.assertEquals(record.getSchema().getIdentifier(), record2.getSchema().getIdentifier());
    Assert.assertEquals((long) record.getRecordVersion(), record2.getRecordVersion() - 1l);// version should be 1 higher
    if (record.getAcl() != null) {
      Assert.assertTrue(record.getAcl().containsAll(record2.getAcl()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
  }

  @Test
  public void testUpdateRecordWithoutDocument() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    MetadataRecord record = mapper.readValue(body, MetadataRecord.class);
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata/" + metadataRecordId).
            file(recordFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andReturn();
//    this.mockMvc.perform(put("/api/v1/metadata/dc").contentType("application/json").header("If-Match", etag).contentType(MetadataRecord.METADATA_RECORD_MEDIA_TYPE).content(mapper.writeValueAsString(record))).andDo(print()).andExpect(status().isBadRequest()).andReturn();
    body = result.getResponse().getContentAsString();

    MetadataRecord record2 = mapper.readValue(body, MetadataRecord.class);
    Assert.assertEquals(record.getDocumentHash(), record2.getDocumentHash());
    Assert.assertEquals(record.getCreatedAt(), record2.getCreatedAt());
    Assert.assertEquals(record.getMetadataDocumentUri(), record2.getMetadataDocumentUri());
    Assert.assertEquals(record.getSchema().getIdentifier(), record2.getSchema().getIdentifier());
    Assert.assertEquals(record.getRecordVersion(), record2.getRecordVersion());// version should be  the same
    if (record.getAcl() != null) {
      Assert.assertTrue(record.getAcl().containsAll(record2.getAcl()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
  }

  @Test
  public void testDeleteRecordWithoutAuthentication() throws Exception {
    String metadataRecordId = createDCMetadataRecord();

    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");

    this.mockMvc.perform(delete("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("If-Match", etag)).andDo(print()).andExpect(status().isNoContent()).andReturn();
    //delete second time
    this.mockMvc.perform(delete("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isPreconditionRequired()).andReturn();
  }

  @Test
  public void testDeleteRecord() throws Exception {
    String metadataRecordId = createDCMetadataRecord();

    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");

    this.mockMvc.perform(delete("/api/v1/metadata/" + metadataRecordId).header("If-Match", etag).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isNoContent()).andReturn();
    //delete second time
    this.mockMvc.perform(delete("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isPreconditionRequired()).andReturn();
//    Recreation should be no problem.
//    //try to create after deletion (Should return HTTP GONE)
//    MetadataRecord record = new MetadataRecord();
//    record.setSchemaId("dc");
//    record.setRelatedResource(RELATED_RESOURCE);
//    ObjectMapper mapper = new ObjectMapper();
//
//    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
//    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());
//
//    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata/" + METADATA_RECORD_ID).
//            file(recordFile).
//            file(metadataFile)).andDo(print()).andExpect(status().isGone()).andReturn();
  }


  @Test
  public void testGetAllVersionsOfRecord() throws Exception {
    String metadataRecordId = createDCMetadataRecord();
    // Get version of record as array
    // Read all versions (only 1 version available)
    this.mockMvc.perform(get("/api/v1/metadata/" ).param("id", metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header(HttpHeaders.ACCEPT, "application/json")).andDo(print()).andExpect(status().isOk()).andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)));

    MvcResult result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("Accept", MetadataRecord.METADATA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    String etag = result.getResponse().getHeader("ETag");
    String body = result.getResponse().getContentAsString();

    ObjectMapper mapper = new ObjectMapper();
    MetadataRecord record = mapper.readValue(body, MetadataRecord.class);
    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT_VERSION_2.getBytes());

    result = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata/" + record.getId()).
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header("If-Match", etag).with(putMultipart())).andDo(print()).andExpect(status().isOk()).andReturn();

//    result = this.mockMvc.perform(put("/api/v1/metadata/dc").header("If-Match", etag).contentType(MetadataRecord.METADATA_RECORD_MEDIA_TYPE).content(mapper.writeValueAsString(record))).andDo(print()).andExpect(status().isOk()).andReturn();
    body = result.getResponse().getContentAsString();

    MetadataRecord record2 = mapper.readValue(body, MetadataRecord.class);
    Assert.assertNotEquals(record.getDocumentHash(), record2.getDocumentHash());//mime type was changed by update
    Assert.assertEquals(record.getCreatedAt(), record2.getCreatedAt());
    Assert.assertEquals(record.getSchema().getIdentifier(), record2.getSchema().getIdentifier());
    Assert.assertEquals((long) record.getRecordVersion(), record2.getRecordVersion() - 1l);// version should be 1 higher
    if (record.getAcl() != null) {
      Assert.assertTrue(record.getAcl().containsAll(record2.getAcl()));
    }
    Assert.assertTrue(record.getLastUpdate().isBefore(record2.getLastUpdate()));
    // Check for new metadata document.
    result = this.mockMvc.perform(get("/api/v1/metadata/" + metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isOk()).andReturn();
    String content = result.getResponse().getContentAsString();

    String dcMetadata = DC_DOCUMENT_VERSION_2;

    Assert.assertEquals(dcMetadata, content);

    Assert.assertEquals(record.getMetadataDocumentUri().replace("version=1", "version=2"), record2.getMetadataDocumentUri());
    // Get version of record as array
    // Read all versions (only 1 version available)
    this.mockMvc.perform(get("/api/v1/metadata").param("id", metadataRecordId).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken).header(HttpHeaders.ACCEPT, "application/json")).andDo(print()).andExpect(status().isOk()).andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)));
  }

  private String createDCMetadataRecord() throws Exception {
    MetadataRecord record = new MetadataRecord();
//    record.setId("my_id");
    record.setSchema(ResourceIdentifier.factoryInternalResourceIdentifier(SCHEMA_ID));
    record.setRelatedResource(RELATED_RESOURCE);
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("SELF",PERMISSION.READ));
    aclEntries.add(new AclEntry("test2",PERMISSION.ADMINISTRATE));
    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "metadata-record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile metadataFile = new MockMultipartFile("document", "metadata.xml", "application/xml", DC_DOCUMENT.getBytes());

    MvcResult andReturn = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/metadata").
            file(recordFile).
            file(metadataFile).header(HttpHeaders.AUTHORIZATION,
            "Bearer " + userToken)).andDo(print()).andExpect(status().isCreated()).andExpect(redirectedUrlPattern("http://*:*/**/*?version=1")).andReturn();
    MetadataRecord result = mapper.readValue(andReturn.getResponse().getContentAsString(), MetadataRecord.class);
    
    return result.getId();
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

  private void ingestSchemaRecord() throws Exception {
    MetadataSchemaRecord record = new MetadataSchemaRecord();
    record.setSchemaId("my_dc");
    record.setType(MetadataSchemaRecord.SCHEMA_TYPE.XML);
    record.setMimeType(MediaType.APPLICATION_XML.toString());
    Set<AclEntry> aclEntries = new HashSet<>();
    aclEntries.add(new AclEntry("test", PERMISSION.READ));
    aclEntries.add(new AclEntry("SELF", PERMISSION.ADMINISTRATE));
    record.setAcl(aclEntries);
    ObjectMapper mapper = new ObjectMapper();

    MockMultipartFile recordFile = new MockMultipartFile("record", "record.json", "application/json", mapper.writeValueAsString(record).getBytes());
    MockMultipartFile schemaFile = new MockMultipartFile("schema", "schema.xsd", "application/xml", DC_SCHEMA.getBytes());

    this.mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1/schemas").
            file(recordFile).
            file(schemaFile)).andDo(print()).andExpect(status().isCreated()).andReturn();
  }

  public static synchronized boolean isInitialized() {
    boolean returnValue = alreadyInitialized;
    alreadyInitialized = Boolean.TRUE;

    return returnValue;
  }

  private String getSchemaUrl(String schemaId) throws Exception {
    MvcResult res = this.mockMvc.perform(get("/api/v1/schemas/" + schemaId).header("Accept", MetadataSchemaRecord.METADATA_SCHEMA_RECORD_MEDIA_TYPE)).andDo(print()).andExpect(status().isOk()).andReturn();
    ObjectMapper map = new ObjectMapper();
    MetadataSchemaRecord result = map.readValue(res.getResponse().getContentAsString(), MetadataSchemaRecord.class);
    return result.getSchemaDocumentUri().replaceFirst("8080", "41408");
  }
}
