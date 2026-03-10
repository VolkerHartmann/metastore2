/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.kit.datamanager.metastore2.dao;

import edu.kit.datamanager.metastore2.domain.SchemaUrl2Path;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;
import java.util.Optional;

/**
 * Database linking URL to local path (if available)
 */
public interface ISchemaUrl2PathDao extends JpaRepository<SchemaUrl2Path, String>, JpaSpecificationExecutor<SchemaUrl2Path>{
  Optional<SchemaUrl2Path>  findByUrl(String url);
  List<SchemaUrl2Path>      findByPath(String path);
  Optional<SchemaUrl2Path>  findBySchemaIdAndVersion(String schemaId, String version);
  Optional<SchemaUrl2Path>  findFirstBySchemaIdOrderByVersionDesc(String schemaId);
  List<SchemaUrl2Path>      findBySchemaIdOrderByVersionDesc(String schemaId);
}
