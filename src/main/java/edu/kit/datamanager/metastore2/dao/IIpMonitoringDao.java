/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.metastore2.dao;

import edu.kit.datamanager.metastore2.domain.IpMonitoring;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.Optional;

/**
 * Database linking hash of IP address to last visit.
 */
public interface IIpMonitoringDao extends JpaRepository<IpMonitoring, String>, JpaSpecificationExecutor<IpMonitoring> {
  @Modifying
  @Query(nativeQuery = true, value = "DELETE FROM ip_monitoring m WHERE m.last_visit < :lastDate")
  void deleteAllEntriesOlderThan(Instant lastDate);
}