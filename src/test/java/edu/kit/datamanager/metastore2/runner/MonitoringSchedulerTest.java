/*
 * Copyright 2025 Karlsruhe Institute of Technology.
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
package edu.kit.datamanager.metastore2.runner;


import edu.kit.datamanager.metastore2.configuration.MonitoringConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MonitoringSchedulerTest {
  MonitoringConfiguration monitoringConfiguration;
  MonitoringScheduler monitoringScheduler;

  @Before
  public void setUp() {
    monitoringConfiguration = new MonitoringConfiguration();
    monitoringConfiguration.setEnabled(true);
    monitoringConfiguration.setCron4schedule("* * * * * *"); // Every second
    monitoringConfiguration.setCron4cleanUp("*/2 * * * * *"); // Every two seconds
    monitoringScheduler = new MonitoringScheduler(monitoringConfiguration, null);
    monitoringScheduler.scheduleMonitoring();
  }

  @Test
  public void testMonitoringSchedulerWithMonitoringServiceIsNull() throws Exception {
    Thread.sleep(3000); // Wait for the monitoring scheduler to run at least once
    monitoringConfiguration.setEnabled(false);
    Thread.sleep(3000); // Wait for the monitoring scheduler to run again
  }

  @After
  public void tearDown() {
    // Shutdown the scheduler to clean up resources
    monitoringScheduler.shutdown();
  }
}
