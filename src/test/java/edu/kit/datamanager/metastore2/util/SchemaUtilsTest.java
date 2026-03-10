/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.metastore2.util;

import edu.kit.datamanager.metastore2.domain.MetadataSchemaRecord;
import edu.kit.datamanager.metastore2.test.CreateSchemaUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.http.MediaType;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 *
 * @author hartmann-v
 */
public class SchemaUtilsTest {

  private final static String KIT_SCHEMA = CreateSchemaUtil.KIT_SCHEMA;

  public SchemaUtilsTest() {
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

  @Test
  public void testConstructor() {
    assertNotNull(new SchemaUtils());
  }

  /**
   * Test of guessType method, of class SchemaUtils.
   */
  @Test
  public void testGuessTypeWithNullInput() {
    System.out.println("guessType for NULL");
    byte[] schema = null;
    String result = SchemaUtils.guessMimetype(schema);
    assertNull(result);
  }

  /**
   * Test of guessType method, of class SchemaUtils.
   */
  @Test
  public void testGuessTypeWithEmptyInput() {
    System.out.println("guessType for empty input");
    byte[] schema = "".getBytes();
    String result = SchemaUtils.guessMimetype(schema);
    assertNull(result);
  }

  /**
   * Test of guessType method, of class SchemaUtils.
   */
  @Test
  public void testGuessTypeXMLLongSchema() {
    System.out.println("guessType for XML");
    byte[] schema = KIT_SCHEMA.getBytes(StandardCharsets.UTF_8);
    String expResult = MediaType.APPLICATION_XML_VALUE;
    String result = SchemaUtils.guessMimetype(schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testGuessTypeXML() {
    System.out.println("guessType for XML");
    byte[] schema = null;
    String expResult = MediaType.APPLICATION_XML_VALUE;
    String[] patterns = {"<?xml version=\"1.0\" encoding=\"UTF-8\" ?> \n  <xs:schema ", "<xs:schema=", " <xs:schema=", " < xs:schema=", " <schema=", "< schema=", " < schema=", " < sch:schema = "};
    for (String beginning : patterns) {
      schema = beginning.getBytes(StandardCharsets.UTF_8);
      String result = SchemaUtils.guessMimetype(schema);
      assertEquals(expResult, result);
    }
  }

  @Test
  public void testGuessTypeNullXML() {
    System.out.println("guessType is neither XML nor JSON");
    byte[] schema = null;
    String[] patterns = {"<?xml version=\"1.0\">\n<myschema> \n<xs:schema ", "<?xml version=\"1.0\" encoding=\"UTF-8\" ?> \n <xsschema ", " <sch:ema=", "< d:schema=", " < longprefix:schema=", " < schem=a = "};
    for (String beginning : patterns) {
      schema = beginning.getBytes(StandardCharsets.UTF_8);
      String result = SchemaUtils.guessMimetype(schema);
      assertNull(result);
    }
  }

  /**
   * Test of guessType method, of class SchemaUtils.
   */
  @Test
  public void testGuessTypeJSON() {
    System.out.println("guessType for JSON");
    byte[] schema = null;
    String expResult =MediaType.APPLICATION_JSON_VALUE;
    String[] patterns = {"{ \"$schema\" : \"https://...", "{\n \"$schema\": ", "{ \"$id\" : \"...", "{\n \"$id\" : \"...", "\n{ \"$schema\" : \"https://...", "{ \"$schema\" : \"https://..."};
    for (String beginning : patterns) {
      schema = beginning.getBytes(StandardCharsets.UTF_8);
      String result = SchemaUtils.guessMimetype(schema);
      assertEquals(expResult, result);
    }
  }

  @Test
  public void testGuessTypeNullJSON() {
    System.out.println("guessType is neither XML nor JSON");
    byte[] schema = null;
    String[] patterns = {"<?xml version=\"1.0\">\n{ \"$schema\" : \"https://...", "schema: { \"$schema\" : \"https://...", "{ \"schema\" : \"https://...", "{\n \"schema\": ", "{ \"id\" : \"...", "{\n \"id\" : \"...", "\n{[ \"$schema\" : \"https://...", "{{ \"$schema\" : \"https://..."};
    for (String beginning : patterns) {
      schema = beginning.getBytes(StandardCharsets.UTF_8);
      String result = SchemaUtils.guessMimetype(schema);
      assertNull(result);
    }
  }

  @Test
  public void testGetTargetNamespaceFromSchema() {
    System.out.println("getTargetNamespaceFromSchema");
    byte[] schema = KIT_SCHEMA.getBytes(StandardCharsets.UTF_8);
    String expResult = "http://www.example.org/kit";
    String result = SchemaUtils.getTargetNamespaceFromSchema(schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTargetNamespaceInvalidSource() {
    System.out.println("getTargetNamespaceFromSchema with invalid source");
    byte[] schema = "This is no XML schema".getBytes(StandardCharsets.UTF_8);
    String result = SchemaUtils.getTargetNamespaceFromSchema(schema);
    assertNull(result);
  }

}
