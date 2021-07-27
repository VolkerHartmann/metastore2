/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.metastore2.util;

import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord.SCHEMA_TYPE;
import edu.kit.datamanager.metastore2.domain.ResourceIdentifier;
import edu.kit.datamanager.repo.configuration.RepoBaseConfiguration;
import edu.kit.datamanager.repo.domain.DataResource;
import edu.kit.datamanager.repo.domain.acl.AclEntry;
import java.time.Instant;
import java.util.Set;
import static jdk.nashorn.internal.objects.NativeJava.type;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author hartmann-v
 */
public class MetadataSchemaRecordUtilTest {
  
  public MetadataSchemaRecordUtilTest() {
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
   * Test of migrateToDataResource method, of class MetadataSchemaRecordUtil.
   */
  @Test
  public void testMigrateToDataResource() {
    System.out.println("migrateToDataResource");
    RepoBaseConfiguration applicationProperties = null;
    MetadataSchemaRecord metadataSchemaRecord = null;
    DataResource expResult = null;
    DataResource result = MetadataSchemaRecordUtil.migrateToDataResource(applicationProperties, metadataSchemaRecord);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of migrateToMetadataSchemaRecord method, of class MetadataSchemaRecordUtil.
   */
  @Test
  public void testMigrateToMetadataSchemaRecord() {
    System.out.println("migrateToMetadataSchemaRecord");
    RepoBaseConfiguration applicationProperties = null;
    DataResource dataResource = null;
    boolean provideETag = false;
    MetadataSchemaRecord expResult = null;
    MetadataSchemaRecord result = MetadataSchemaRecordUtil.migrateToMetadataSchemaRecord(applicationProperties, dataResource, provideETag);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of mergeRecords method, of class MetadataSchemaRecordUtil.
   */
  @Test
  public void testMergeRecords() {
    System.out.println("mergeRecords");
    MetadataSchemaRecord managed = null;
    MetadataSchemaRecord provided = null;
    MetadataSchemaRecord expResult = null;
    MetadataSchemaRecord result = MetadataSchemaRecordUtil.mergeRecords(managed, provided);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  private MetadataSchemaRecord buildMSR(Set<AclEntry> aclEntry, String comment, Instant creationDate, String definition,
          String eTag, String label, Instant update, boolean doNotSync, String mimetype, String pid, String schemaDocument,
          String schemaHash, String schemaId, Long version, SCHEMA_TYPE type) {
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
    msr.setPid(ResourceIdentifier.factoryInternalResourceIdentifier(pid));
    msr.setSchemaDocumentUri(schemaDocument);
    msr.setSchemaHash(schemaHash);
    msr.setSchemaId(schemaId);
    msr.setSchemaVersion(version);
    msr.setType(type);
   
    return msr;
            
  }
}
