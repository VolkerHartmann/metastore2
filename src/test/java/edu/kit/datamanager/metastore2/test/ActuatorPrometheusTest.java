/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.metastore2.test;

import edu.kit.datamanager.metastore2.configuration.MetastoreConfiguration;
import edu.kit.datamanager.metastore2.runner.MonitoringScheduler;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
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
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.IOException;

import static org.springframework.restdocs.mockmvc.MockMvcRestDocumentation.documentationConfiguration;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
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
@TestPropertySource(properties = {"server.port=41426"})
@TestPropertySource(properties = {"metastore.schema.schemaFolder=file:///tmp/metastore2/prometheus/schema"})
@TestPropertySource(properties = {"metastore.metadata.metadataFolder=file:///tmp/metastore2/prometheus/metadata"})
@TestPropertySource(properties = {"spring.datasource.url=jdbc:h2:mem:db_prometheus;DB_CLOSE_DELAY=-1;MODE=LEGACY;NON_KEYWORDS=VALUE"})
@TestPropertySource(properties = {"spring.jpa.database-platform=org.hibernate.dialect.H2Dialect"})
@TestPropertySource(properties = {"spring.jpa.defer-datasource-initialization=true"})
@TestPropertySource(properties = {"metastore.monitoring.enabled=true"})
@TestPropertySource(properties = {"management.endpoint.prometheus.enabled=true"})
@TestPropertySource(properties = {"management.endpoints.web.exposure.include=info,health,prometheus"})
@TestPropertySource(properties = {"metastore.monitoring.cron4schedule=* * * * * *"})
@TestPropertySource(properties = {"metastore.monitoring.cron4cleanUp=*/2 * * * * *"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureObservability
public class ActuatorPrometheusTest {

  private final static String TEMP_DIR_4_ALL = "/tmp/metastore2/prometheus/";

  private static Boolean alreadyInitialized = Boolean.FALSE;
  @Rule
  public JUnitRestDocumentation restDocumentation = new JUnitRestDocumentation();
  private MockMvc mockMvc;
  @Autowired
  private WebApplicationContext context;
  @Autowired
  private MetastoreConfiguration metadataConfig;
  @Autowired
  private MonitoringScheduler monitoringScheduler;

  public static synchronized boolean isInitialized() {
    boolean returnValue = alreadyInitialized;
    alreadyInitialized = Boolean.TRUE;

    return returnValue;
  }

  @Before
  public void setUp() throws Exception {
    System.out.println("------ActuatorPrometheusTest--------------------------");
    System.out.println("------" + this.metadataConfig);
    System.out.println("------------------------------------------------------");

    try {
      // setup mockMvc
      this.mockMvc = MockMvcBuilders.webAppContextSetup(this.context)
              .apply(springSecurity())
              .apply(documentationConfiguration(this.restDocumentation).uris()
                      .withPort(41416))
              .build();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @After
  public void tearDown() throws IOException {
    monitoringScheduler.shutdown(); // ensure that the monitoring scheduler is stopped
  }

  @Test
  public void testForNotExposedActuators() throws Exception {
    this.mockMvc.perform(get("/actuator/beans")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/caches")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/conditions")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/configprops")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/env")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/loggers")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/heapdump")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/threaddump")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/metrics")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/scheduledtasks")).andDo(print()).andExpect(status().isNotFound());
    this.mockMvc.perform(get("/actuator/mappings")).andDo(print()).andExpect(status().isNotFound());
//    this.mockMvc.perform(get("/actuator/prometheus")).andDo(print()).andExpect(status().isNotFound());
  }

  @Test
  public void testActuator() throws Exception {
    // test for endpoint /actuator/prometheus
    Thread.sleep(3000); // wait for the monitoring scheduler to run at least once

    MvcResult result = this.mockMvc.perform(get("/actuator/prometheus")).andDo(print()).andExpect(status().isOk())
            .andExpect(content().string(Matchers.containsString("# TYPE metastore_metadata_documents")))
            .andExpect(content().string(Matchers.containsString("# TYPE metastore_metadata_schemas")))
            .andExpect(content().string(Matchers.containsString("# TYPE metastore_requests_served_total")))
            .andExpect(content().string(Matchers.containsString("# TYPE metastore_unique_users")))
            .andReturn();
  }
}
