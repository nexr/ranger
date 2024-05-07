/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.trino;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.HadoopConfigHolder;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.trino.client.TrinoResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RangerServiceTrino extends RangerBaseService {
  private static final Logger LOG = LoggerFactory.getLogger(RangerServiceTrino.class);
  public static final String RESOURCE_CATALOG   = "catalog";
  public static final String RESOURCE_SCHEMA    = "schema";
  public static final String RESOURCE_TABLE     = "table";
  public static final String RESOURCE_FUNCTION  = "function";
  public static final String RESOURCE_COLUMN    = "column";
  public static final String RESOURCE_SYSTEMPROPERTY    = "systemproperty";
  public static final String RESOURCE_SESSIONPROPERTY    = "sessionproperty";
  public static final String RESOURCE_PROCEDURE = "procedure";
  public static final String ACCESS_TYPE_ALTER = "alter";
  public static final String ACCESS_TYPE_CREATE = "create";
  public static final String ACCESS_TYPE_SELECT = "select";
  public static final String ACCESS_TYPE_READ  = "read";
  public static final String ACCESS_TYPE_USE  = "use";
  public static final String ACCESS_TYPE_SHOW = "show";
  public static final String ACCESS_TYPE_EXECUTE = "execute";
  public static final String ACCESS_TYPE_ALL    = "all";
  public static final String WILDCARD_ASTERISK  = "*";

  public static final String ALL_CATALOG_INFORMATIONSCHEMA = "information_schema";
  public static final String ALL_CATALOG_INFORMATION_SCHEMA_DB_POLICYNAME = "all - catalog information_schema schema tables columns";

  public static final String ALL_CATALOG_USER_SCHEMA_DB_POLICYNAME = "all - catalog {USER} schema tables columns";

  public static final String SPARK_CATALOG_DEFAULT_NAME = "spark";
  public static final String SPARK_DB_DEFAULT   		        = "default";
  public static final String SPARK_DEFAULT_DB_POLICYNAME = "spark catalog default schema tables columns";

  public static final String SPARK_USERHOME_DB_POLICYNAME = "spark catalog {USER} schema tables columns";

  public static final String ICEBERG_CATALOG_DEFAULT_NAME = "iceberg";
  public static final String ICEBERG_DB_DEFAULT   		        = "default";
  public static final String ICEBERG_DEFAULT_DB_POLICYNAME = "iceberg catalog default schema tables columns";
  public static final String ICEBERG_DB_SYSTEM   		        = "system";
  public static final String ICEBERG_SYSTEM_DB_POLICYNAME = "iceberg catalog system schema tables columns";

  public static final String TRINO_SYSTEM_CATALOG        = "system";
  public static final String TRINO_SYSTEM_CATALOG_POLICYNAME = "system catalog schema tables columns";

  @Override
  public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==> RangerServiceTrino.getDefaultRangerPolicies()");
    }

    List<RangerPolicy> ret = super.getDefaultRangerPolicies();
    for (RangerPolicy defaultPolicy : ret) {
      final Map<String, RangerPolicy.RangerPolicyResource> policyResources = defaultPolicy.getResources();
      if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
        RangerPolicy.RangerPolicyItem policyItemForLookupUser = new RangerPolicy.RangerPolicyItem();
        policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));
        policyItemForLookupUser.setAccesses(Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_READ)));
        policyItemForLookupUser.setDelegateAdmin(false);
        defaultPolicy.getPolicyItems().add(policyItemForLookupUser);
      }

      if (policyResources.size() == 1 && hasWildcardAsteriskResource(policyResources, RESOURCE_CATALOG)) { // policy for all catalog
        RangerPolicy.RangerPolicyItem policyItemPublic = new RangerPolicy.RangerPolicyItem();

        policyItemPublic.setGroups(Collections.singletonList(RangerPolicyEngine.GROUP_PUBLIC));
        List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
        accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SELECT));
        accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_USE));
        accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SHOW));
        policyItemPublic.setAccesses(accesses);
        defaultPolicy.getPolicyItems().add(policyItemPublic);
      } else if (policyResources.size() == 1 && hasWildcardAsteriskResource(policyResources, RESOURCE_FUNCTION)) { // policy for function
        RangerPolicy.RangerPolicyItem policyItemUser = new RangerPolicy.RangerPolicyItem();

        policyItemUser.setUsers(Collections.singletonList(RangerPolicyEngine.USER_CURRENT));
        policyItemUser.setAccesses(Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_EXECUTE)));
        policyItemUser.setDelegateAdmin(true);

        defaultPolicy.getPolicyItems().add(policyItemUser);
//      } else if (policyResources.size() == 3 && hasWildcardAsteriskResource(policyResources, RESOURCE_CATALOG, RESOURCE_SCHEMA, RESOURCE_PROCEDURE)) { // policy for procedure
//        // add nothing
//      }
//      else if ((policyResources.size() == 3 && hasWildcardAsteriskResource(policyResources, RESOURCE_CATALOG, RESOURCE_SCHEMA, RESOURCE_TABLE)) ||                  // policy for all tables
//              (policyResources.size() == 4 && hasWildcardAsteriskResource(policyResources, RESOURCE_CATALOG, RESOURCE_SCHEMA, RESOURCE_TABLE, RESOURCE_COLUMN))) { // policy for all columns
//        // add nothing
      } else if (policyResources.size() == 2 && hasWildcardAsteriskResource(policyResources, RESOURCE_CATALOG, RESOURCE_SESSIONPROPERTY)) { // policy for system properties and session properties
        RangerPolicy.RangerPolicyItem policyItemUser = new RangerPolicy.RangerPolicyItem();

        policyItemUser.setUsers(Collections.singletonList(RangerPolicyEngine.USER_CURRENT));
        policyItemUser.setAccesses(Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_ALTER)));
        policyItemUser.setDelegateAdmin(true);

        defaultPolicy.getPolicyItems().add(policyItemUser);
      }
    }

    //Policy for hive catalog information_schema db
    RangerPolicy allCatalogInformationSchemaDBPolicy = createAllCatalogInformationSchemaDBPolicy();
    ret.add(allCatalogInformationSchemaDBPolicy);

    //Policy for spark catalog default db
    RangerPolicy sparkDefaultDBPolicy = createSparkDefaultDBPolicy();
    ret.add(sparkDefaultDBPolicy);

    //Policy for iceberg catalog default db
    RangerPolicy icebergDefaultDBPolicy = createIcebergDefaultDBPolicy();
    ret.add(icebergDefaultDBPolicy);

    // Policy for system catalog
    RangerPolicy systemCatalogPolicy = createSystemCatalogPolicy();
    ret.add(systemCatalogPolicy);

    //Policy for all catalog user schema db
    RangerPolicy allCatalogUserSchemaDBPolicy = createAllCatalogUserSchemaDBPolicy();
    ret.add(allCatalogUserSchemaDBPolicy);


    if (LOG.isDebugEnabled()) {
      LOG.debug("<== RangerServiceTrino.getDefaultRangerPolicies()");
    }
    return ret;
  }

  private boolean hasWildcardAsteriskResource(Map<String, RangerPolicy.RangerPolicyResource> policyResources, String... resourceNames) {
    for (String resourceName : resourceNames) {
      RangerPolicy.RangerPolicyResource resource = policyResources.get(resourceName);
      List<String>         values   = resource != null ? resource.getValues() : null;

      if (values == null || !values.contains(WILDCARD_ASTERISK)) {
        return false;
      }
    }
    return true;
  }

  private RangerPolicy createSystemCatalogPolicy() {
    RangerPolicy systemCatalogPolicy = new RangerPolicy();

    systemCatalogPolicy.setName(TRINO_SYSTEM_CATALOG_POLICYNAME);
    systemCatalogPolicy.setService(serviceName);

    // resources
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put(RESOURCE_CATALOG, new RangerPolicy.RangerPolicyResource(Arrays.asList(TRINO_SYSTEM_CATALOG), false, false));
    resources.put(RESOURCE_SCHEMA, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));
    resources.put(RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));
    resources.put(RESOURCE_COLUMN, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));

    // policy
    List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
    accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SELECT));
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem(accesses, null, Arrays.asList(RangerPolicyEngine.GROUP_PUBLIC), null, null, false);

    systemCatalogPolicy.setResources(resources);
    systemCatalogPolicy.setPolicyItems(Collections.singletonList(item));

    return systemCatalogPolicy;
  }

  private RangerPolicy createSparkDefaultDBPolicy() {
    RangerPolicy defaultDBPolicy = new RangerPolicy();

    defaultDBPolicy.setName(SPARK_DEFAULT_DB_POLICYNAME);
    defaultDBPolicy.setService(serviceName);

    // resources
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put(RESOURCE_CATALOG, new RangerPolicy.RangerPolicyResource(Arrays.asList(SPARK_CATALOG_DEFAULT_NAME), false, false));
    resources.put(RESOURCE_SCHEMA, new RangerPolicy.RangerPolicyResource(Arrays.asList(SPARK_DB_DEFAULT), false, false));
    resources.put(RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));
    resources.put(RESOURCE_COLUMN, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));

    // policy
    List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
    accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_CREATE));
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem(accesses, null, Arrays.asList(RangerPolicyEngine.GROUP_PUBLIC), null, null, false);

    defaultDBPolicy.setResources(resources);
    defaultDBPolicy.setPolicyItems(Collections.singletonList(item));

    return defaultDBPolicy;
  }

  private RangerPolicy createAllCatalogUserSchemaDBPolicy() {
    RangerPolicy defaultDBPolicy = new RangerPolicy();

    defaultDBPolicy.setName(ALL_CATALOG_USER_SCHEMA_DB_POLICYNAME);
    defaultDBPolicy.setService(serviceName);

    // resources
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put(RESOURCE_CATALOG, new RangerPolicy.RangerPolicyResource(Arrays.asList(WILDCARD_ASTERISK), false, false));
    resources.put(RESOURCE_SCHEMA, new RangerPolicy.RangerPolicyResource(Arrays.asList(RangerPolicyEngine.USER_CURRENT), false, false));
    resources.put(RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));
    resources.put(RESOURCE_COLUMN, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));

    // policy
    List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
    accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_ALL));
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem(accesses, Arrays.asList(RangerPolicyEngine.USER_CURRENT), null, null, null, false);

    defaultDBPolicy.setResources(resources);
    defaultDBPolicy.setPolicyItems(Collections.singletonList(item));

    return defaultDBPolicy;
  }

  private RangerPolicy createAllCatalogInformationSchemaDBPolicy() {
    RangerPolicy defaultDBPolicy = new RangerPolicy();

    defaultDBPolicy.setName(ALL_CATALOG_INFORMATION_SCHEMA_DB_POLICYNAME);
    defaultDBPolicy.setService(serviceName);

    // resources
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put(RESOURCE_CATALOG, new RangerPolicy.RangerPolicyResource(Arrays.asList(WILDCARD_ASTERISK), false, false));
    resources.put(RESOURCE_SCHEMA, new RangerPolicy.RangerPolicyResource(Arrays.asList(ALL_CATALOG_INFORMATIONSCHEMA), false, false));
    resources.put(RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));
    resources.put(RESOURCE_COLUMN, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));

    // policy
    List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
    accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SELECT));
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem(accesses, null, Arrays.asList(RangerPolicyEngine.GROUP_PUBLIC), null, null, false);

    defaultDBPolicy.setResources(resources);
    defaultDBPolicy.setPolicyItems(Collections.singletonList(item));

    return defaultDBPolicy;
  }

  private RangerPolicy createIcebergDefaultDBPolicy() {
    RangerPolicy defaultDBPolicy = new RangerPolicy();

    defaultDBPolicy.setName(ICEBERG_DEFAULT_DB_POLICYNAME);
    defaultDBPolicy.setService(serviceName);

    // resources
    Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
    resources.put(RESOURCE_CATALOG, new RangerPolicy.RangerPolicyResource(Arrays.asList(ICEBERG_CATALOG_DEFAULT_NAME), false, false));
    resources.put(RESOURCE_SCHEMA, new RangerPolicy.RangerPolicyResource(Arrays.asList(ICEBERG_DB_DEFAULT), false, false));
    resources.put(RESOURCE_TABLE, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));
    resources.put(RESOURCE_COLUMN, new RangerPolicy.RangerPolicyResource(WILDCARD_ASTERISK));

    // policy
    List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
    accesses.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_CREATE));
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem(accesses, null, Arrays.asList(RangerPolicyEngine.GROUP_PUBLIC), null, null, false);

    defaultDBPolicy.setResources(resources);
    defaultDBPolicy.setPolicyItems(Collections.singletonList(item));

    return defaultDBPolicy;
  }

  @Override
  public Map<String, Object> validateConfig() throws Exception {
    Map<String, Object> ret = new HashMap<String, Object>();
    String serviceName = getServiceName();

    if (LOG.isDebugEnabled()) {
      LOG.debug("RangerServiceTrino.validateConfig(): Service: " +
        serviceName);
    }

    if (configs != null) {
      try {
        if (!configs.containsKey(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
          configs.put(HadoopConfigHolder.RANGER_LOGIN_PASSWORD, null);
        }
        ret = TrinoResourceManager.connectionTest(serviceName, configs);
      } catch (HadoopException he) {
        LOG.error("<== RangerServiceTrino.validateConfig() Error:" + he);
        throw he;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("RangerServiceTrino.validateConfig(): Response: " +
        ret);
    }
    return ret;
  }

  @Override
  public List<String> lookupResource(ResourceLookupContext context) throws Exception {

    List<String> ret         = new ArrayList<String>();
    String  serviceName      = getServiceName();
    String	serviceType		   = getServiceType();
    Map<String,String> configs = getConfigs();
    if(LOG.isDebugEnabled()) {
      LOG.debug("==> RangerServiceTrino.lookupResource() Context: (" + context + ")");
    }
    if (context != null) {
      try {
        if (!configs.containsKey(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
          configs.put(HadoopConfigHolder.RANGER_LOGIN_PASSWORD, null);
        }
        ret  = TrinoResourceManager.getTrinoResources(serviceName, serviceType, configs,context);
      } catch (Exception e) {
        LOG.error( "<==RangerServiceTrino.lookupResource() Error : " + e);
        throw e;
      }
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("<== RangerServiceTrino.lookupResource() Response: (" + ret + ")");
    }
    return ret;
  }

}
