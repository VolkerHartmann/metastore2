/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.entities.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.kit.datamanager.entities.Identifier;
import edu.kit.datamanager.exceptions.MessageValidationException;
import edu.kit.datamanager.metastore2.util.DataResourceRecordUtil;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.repo.domain.RelatedIdentifier;
import org.junit.*;

import static org.junit.Assert.*;

/**
 *
 */
public class MetadataResourceMessageTest {
  
  public MetadataResourceMessageTest() {
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
  
  
  /*
      MetadataRecord mdr = new MetadataRecord();
    mdr.setId("myId");
    mdr.setMetadataDocumentUri("https://www.example.org/api/v1/metadata/anyId");
    mdr.setSchemaId("my_dc");
    MetadataResourceMessage msg = MetadataResourceMessage.factoryCreateMetadataMessage(mdr, "me", "you");
    System.out.println(msg.toJson());
    System.out.println(msg.getRoutingKey());

  */

  /**
   * Test of factoryCreateMetadataMessage method, of class MetadataResourceMessage.
   */
  @Test
  public void testConstructor() {
    MetadataResourceMessage mdrm = new MetadataResourceMessage();
    assertNotNull(mdrm);
  }

  /**
   * Test of factoryCreateMetadataMessage method, of class MetadataResourceMessage.
   */
  @Test
  public void testFactoryCreateMetadataMessage() throws JsonProcessingException {
    System.out.println("factoryCreateMetadataMessage");
    DataResource metadataRecord;
    String caller = "anyCaller";
    String sender = "anySender";
    String[] ids = {"id1", "id2", "id3", "id4"};
    String[] uris= {null, null, "uri1", "uri2"};
    String[] types = {null, "type1", null, "type2"};
    String action = DataResourceMessage.ACTION.CREATE.getValue();
    for (int index = 0; index < ids.length; index++) {
    String id = ids[index];
    String uri = uris[index];
    String type = types[index];
    metadataRecord = buildDataResourceRecord(id, uri, type);
    MetadataResourceMessage result = MetadataResourceMessage.factoryCreateMetadataMessage(metadataRecord, caller, sender);
    checkJsonString(result, sender, caller, action, id, uri, type);
    }
  }

  /**
   * Test of factoryCreateMetadataMessage method, of class MetadataResourceMessage.
   */
  @Test
  public void testActionIsNull() throws JsonProcessingException {
    System.out.println("factoryCreateMetadataMessage");
    DataResource metadataRecord;
    String caller = "anyCaller";
    String sender = "anySender";
    String[] ids = {"id1"};
    String[] versions= {"0.0.2"};
    String[] types = {"type2"};
    String action = null;
    for (int index = 0; index < ids.length; index++) {
    String id = ids[index];
    String uri = versions[index];
    String type = types[index];
    metadataRecord = buildDataResourceRecord(id, uri, type);
    MetadataResourceMessage result = MetadataResourceMessage.createMessage(metadataRecord, null, DataResourceMessage.SUB_CATEGORY.DATA, uri, sender);
    try {
      checkJsonString(result, sender, caller, action, id, uri, type);
      fail();
    } catch (MessageValidationException mve) {
      assertTrue(mve.getMessage().contains("must not be null"));
    }
    }
  }

  @Test
  public void testIdIsNull() {
    System.out.println("testIdIsNull");
    DataResource metadataRecord;
    String caller = "anyCaller";
    String sender = "anySender";
    String[] ids = {null, null, null, null};
    String[] versions= {null, null, "0.0.1", "0.0.2"};
    String[] types = {null, "type1", null, "type2"};
    for (int index = 0; index < ids.length; index++) {
      String id = ids[index];
      String uri = versions[index];
      String type = types[index];
      metadataRecord = buildDataResourceRecord(id, uri, type);
      try {
        MetadataResourceMessage.factoryCreateMetadataMessage(metadataRecord, caller, sender);
        fail();
      } catch (IllegalArgumentException iae) {
        assertTrue(iae.getMessage().contains("Illegal character in path"));
      }
    }
  }
  @Test
  public void testMetadataRecordIsNull() throws JsonProcessingException {
    System.out.println("testIdIsNull");
    DataResource metadataRecord = null;
    String caller = "anyCaller";
    String sender = "anySender";
    String action = DataResourceMessage.ACTION.CREATE.getValue();
    String id = null;
    String uri = null;
    String type = null;
    MetadataResourceMessage result = MetadataResourceMessage.factoryCreateMetadataMessage(metadataRecord, caller, sender);
    try {
      checkJsonString(result, sender, caller, action, id, uri, type);
      fail();
    } catch (MessageValidationException mve) {
      assertTrue(mve.getMessage().contains("must not be null"));
    }
  }

  /**
   * Test of factoryUpdateMetadataMessage method, of class MetadataResourceMessage.
   */
  @Test
  public void testFactoryUpdateMetadataMessage() throws JsonProcessingException {
    System.out.println("factoryUpdateMetadataMessage");
    DataResource metadataRecord;
    String caller = "anyCaller";
    String sender = "anySender";
    String[] ids = {"id1", "id2", "id3", "id4"};
    String[] versions= {null, null, "0.0.1", "0.0.2"};
    String[] types = {null, "type1", null, "type2"};
    String action = DataResourceMessage.ACTION.UPDATE.getValue();
    for (int index = 0; index < ids.length; index++) {
    String id = ids[index];
    String uri = versions[index];
    String type = types[index];
    metadataRecord = buildDataResourceRecord(id, uri, type);
    MetadataResourceMessage result = MetadataResourceMessage.factoryUpdateMetadataMessage(metadataRecord, caller, sender);
    checkJsonString(result, sender, caller, action, id, uri, type);
    }
  }

  /**
   * Test of factoryDeleteMetadataMessage method, of class MetadataResourceMessage.
   */
  @Test
  public void testFactoryDeleteMetadataMessage() throws JsonProcessingException {
    System.out.println("factoryDeleteMetadataMessage");
    DataResource metadataRecord;
    String caller = "anyCaller";
    String sender = "anySender";
    String[] ids = {"id1", "id2", "id3", "id4"};
    String[] versions= {null, null, "0.0.1", "0.0.2"};
    String[] types = {null, "type1", null, "type2"};
    String action = DataResourceMessage.ACTION.DELETE.getValue();
    for (int index = 0; index < ids.length; index++) {
    String id = ids[index];
    String uri = versions[index];
    String type = types[index];
    metadataRecord = buildDataResourceRecord(id, uri, type);
    MetadataResourceMessage result = MetadataResourceMessage.factoryDeleteMetadataMessage(metadataRecord, caller, sender);
    checkJsonString(result, sender, caller, action, id, uri, type);
    }
  }

  /**
   * Test of createMessage method, of class MetadataResourceMessage.
   */
  @Test
  public void testCreateMessage() {
    System.out.println("createMessage");
    DataResource metadataRecord = null;
    DataResourceMessage.ACTION action = null;
    DataResourceMessage.SUB_CATEGORY subCategory = null;
    String principal = "";
    String sender = "";
    MetadataResourceMessage expResult = new MetadataResourceMessage();
    MetadataResourceMessage result = MetadataResourceMessage.createMessage(metadataRecord, action, subCategory, principal, sender);
    assertEquals(expResult, result);
  }

  /**
   * Test of equals method, of class MetadataResourceMessage.
   */
  @Test
  public void testEquals() {
    System.out.println("equals");
    Object o = null;
    MetadataResourceMessage instance = new MetadataResourceMessage();
    boolean expResult = false;
    boolean result = instance.equals(o);
    assertEquals(expResult, result);
    expResult = true;
    o = new MetadataResourceMessage();
     result = instance.equals(o);
    assertEquals(expResult, result);
  }

  /**
   * Test of canEqual method, of class MetadataResourceMessage.
   */
  @Test
  public void testCanEqual() {
    System.out.println("canEqual");
    Object other = null;
    MetadataResourceMessage instance = new MetadataResourceMessage();
    boolean expResult = false;
    boolean result = instance.canEqual(other);
    assertEquals(expResult, result);
  }

  /**
   * Test of toString method, of class MetadataResourceMessage.
   */
  @Test
  public void testToString() {
    System.out.println("toString");
    DataResource metadataRecord;
    DataResourceMessage.ACTION action = DataResourceMessage.ACTION.FIX;
    DataResourceMessage.SUB_CATEGORY subCategory = null;
    String principal = "principal";
    String sender = "sender";
     String id = "id";
    String version = "0.0.1";
    String type = "type";
    metadataRecord = buildDataResourceRecord(id, version, type);
   MetadataResourceMessage instance = MetadataResourceMessage.createMessage(metadataRecord, action, subCategory, principal, sender);
    String expResult = new MetadataResourceMessage().toString();
    String result = instance.toString();
    assertEquals(expResult, result);
  }
  
  private DataResource buildDataResourceRecord(String id, String version, String type) {
    DataResource mr = new DataResource();
    mr.setId(id);
    mr.setVersion(version);
    RelatedIdentifier relatedIdentifier4Schema = RelatedIdentifier.factoryRelatedIdentifier(DataResourceRecordUtil.RELATED_SCHEMA_TYPE, type, null, null);
    relatedIdentifier4Schema.setIdentifierType(Identifier.IDENTIFIER_TYPE.INTERNAL);
    mr.getRelatedIdentifiers().add(relatedIdentifier4Schema);
    return mr;
  }
  
  private void checkJsonString(MetadataResourceMessage mdrm, String sender, String caller, String action, String id, String version, String type) throws JsonProcessingException {
    String jsonString = mdrm.toJson();
    System.out.println(jsonString);
    System.out.println("Version: '" + version + "' is not used yet!");
    if (sender != null) {
      assertTrue(jsonString.contains("\"sender\":\"" + sender + "\""));
    } else {
      assertFalse(jsonString.contains("\"sender\":\""));
    }
    if (caller != null) {
      assertTrue(jsonString.contains("\"principal\":\"" + caller + "\""));
    } else {
      assertFalse(jsonString.contains("\"principal\":\""));
    }
    if (action != null) {
      assertTrue(jsonString.contains("\"action\":\"" + action + "\""));
    } else {
      assertFalse(jsonString.contains("\"action\":\""));
    }
    if (id != null) {
      assertTrue(jsonString.contains("\"entityId\":\"" + id + "\""));
      assertTrue(jsonString.contains("\"" + MetadataResourceMessage.RESOLVING_URL_PROPERTY + "\":\""));
    } else {
      assertFalse(jsonString.contains("\"entityId\":\""));
      assertFalse(jsonString.contains("\"" + MetadataResourceMessage.RESOLVING_URL_PROPERTY + "\":\""));
    }
    if (type != null) {
      assertTrue(jsonString.contains("\"" + MetadataResourceMessage.DOCUMENT_TYPE_PROPERTY + "\":\"" + type + "\""));
    } else {
      assertFalse(jsonString.contains("\"" + MetadataResourceMessage.DOCUMENT_TYPE_PROPERTY + "\":\""));
    }

    assertEquals("metadata", mdrm.getEntityName());
    assertTrue(mdrm.getRoutingKey().startsWith("metadata"));
    
  }
  
}
