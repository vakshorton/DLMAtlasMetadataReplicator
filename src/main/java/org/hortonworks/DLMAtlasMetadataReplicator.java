package org.hortonworks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasClassification.AtlasClassifications;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageRelation;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef.AtlasClassificationDefs;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.apache.log4j.*;

import org.apache.commons.codec.binary.Base64;

public class DLMAtlasMetadataReplicator {
	private static String atlasPort = "21000"; // TODO Build a method to pull the Atlas Server port from Ambari Configuration
	private static String hiveServerPort = "10000"; // TODO Build a method to pull the Hive Server port from Ambari Configuration
	
	private static String dpsAdminUserName = null;
	private static String dpsAdminPassword = null;
	private static String ambariAdminUser = null;
	private static String ambariAdminPassword = null;
	private static String atlasUserName = null;
	private static String atlasPassword = null;
	private static String[] atlasBasicAuth = new String[2];
	
	private static String dps_host = null;
	private static String dps_url = null;
	private static String dps_auth_uri = "/auth/in";
	private static String dps_clusters_uri = "/api/actions/clusters?type=all";
	private static String dlm_clusters_uri = "/dlm/api/clusters";
	private static String dlm_policies_uri = "/dlm/api/policies?numResults=200&instanceCount=10";
	private static String dss_collections_uri = "/api/dataset/list/tag/ALL?offset=0&size=10";
	private static String dss_assets_uri = "/api/dataset";
	
	private static String ambari_atlas_component_uri = "/services/ATLAS/components/ATLAS_SERVER";
	private static String ambari_hive_component_uri = "/services/HIVE/components/HIVE_SERVER";
	
	private static String hiveDriver = "org.apache.hive.jdbc.HiveDriver";
	private static String hiveUsername = "hive";
	private static String hivePassword = "hive";
	private static String warehouseDBName = null;
	private static String warehouseLocation = null;
	
	private static int lineageDepth = 0;
	
	static final Logger LOG = Logger.getLogger(DLMAtlasMetadataReplicator.class);
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, AtlasServiceException {
		Map<String, String> env = System.getenv();
		
		if(env.get("DPS_ADMIN_USER_NAME") != null){
			dpsAdminUserName=(String)env.get("DPS_ADMIN_USER_NAME");
		}else {
			LOG.error("DPS_ADMIN_USER_NAME is not set. Please configure environment variables...");
			System.exit(1);
		}

		if(env.get("DPS_ADMIN_PASSWORD") != null){
			dpsAdminPassword = (String)env.get("DPS_ADMIN_PASSWORD");
		}else {
			LOG.error("DPS_ADMIN_PASSWORD is not set. Please configure environment variables...");
			System.exit(1);
		}
		
		if(env.get("AMBARI_ADMIN_USER_NAME") != null){
			ambariAdminUser=(String)env.get("AMBARI_ADMIN_USER_NAME");
		}else {
			LOG.error("AMBARI_ADMIN_USER_NAME is not set. Please configure environment variables...");
			System.exit(1);
		}
		
		if(env.get("AMBARI_ADMIN_PASSWORD") != null){
			ambariAdminPassword = (String)env.get("AMBARI_ADMIN_PASSWORD");
		}else {
			LOG.error("AMBARI_ADMIN_PASSWORD is not set. Please configure environment variables...");
			System.exit(1);
		}
		
		if(env.get("ATLAS_USER_NAME") != null){
			atlasUserName=(String)env.get("ATLAS_USER_NAME");
			atlasBasicAuth[0] = atlasUserName;
		}else {
			LOG.error("ATLAS_USER_NAME is not set. Please configure and environment variables...");
			System.exit(1);
		}
		
		if(env.get("ATLAS_PASSWORD") != null){
			atlasPassword=(String)env.get("ATLAS_PASSWORD");
			atlasBasicAuth[1] = atlasPassword;
		}else {
			LOG.error("ATLAS_PASSWORD is not set. Please configure environment variables...");
			System.exit(1);
		}
		
		if(env.get("DPS_HOST") != null){
			dps_host = (String)env.get("DPS_HOST");
			dps_url = "https://"+dps_host;
		}else {
			LOG.error("DPS_HOST is not set. Please configure environment variables...");
			System.exit(1);
		}
		/*
		if(env.get("WAREHOUSE_DB_NAME") != null){
			warehouseDBName=(String)env.get("WAREHOUSE_DB_NAME");
		}else {
			LOG.error("WAREHOUSE_DB_NAME is not set. Please configure environment variables...");
			System.exit(1);
		}
		
		if(env.get("WAREHOUSE_LOCATION") != null){
			warehouseLocation=(String)env.get("WAREHOUSE_LOCATION");
		}else {
			LOG.error("WAREHOUSE_LOCATION is not set. Please configure environment variables...");
			System.exit(1);
		} */
		
		BasicConfigurator.configure();
		LOG.setLevel(Level.DEBUG);
		initializeTrustManager();
		
		while(true) {
		String token = getToken(dps_url+dps_auth_uri).get(0);
		try {
			//Map<String, JSONObject> dpsClusters = getDpsClusters(token);
			Map<String, JSONObject> dlmClusters = getDlmClusters(token);
			Map<String, JSONObject> dlmPolicies = getDlmPolicies(token);
			 
			for(Entry<String, JSONObject> entry : dlmPolicies.entrySet()) {
				String policyName = entry.getValue().getString("name");
				String sourceDataCenter = entry.getValue().getString("sourceCluster").split("\\$")[0];
				String sourceCluster = entry.getValue().getString("sourceCluster").split("\\$")[1];
				String sourceClusterId = dlmClusters.get(sourceCluster).getString("id");
				String sourceDataset = entry.getValue().getString("sourceDataset");
				String sourceAmbariUrl = dlmClusters.get(sourceCluster).getString("ambariurl");
				
				String targetDataCenter = entry.getValue().getString("targetCluster").split("\\$")[0];
				String targetCluster = entry.getValue().getString("targetCluster").split("\\$")[1];
				String targetClusterId = dlmClusters.get(targetCluster).getString("id");
				String targetDataset = entry.getValue().getString("targetDataset");
				String targetAmbariUrl = dlmClusters.get(targetCluster).getString("ambariurl");
				
				LOG.debug("********** Current DLM Policy Source Dataset (Cluster : Dataset : Ambari Url): " + sourceCluster + " : " + sourceDataset + " : " + sourceAmbariUrl);
				LOG.debug("********** Current DLM Policy Target Dataset (Cluster : Dataset : Ambari Url): " + targetCluster + " : " + targetDataset + " : " + targetAmbariUrl);
				
				List<String> dlmPolicyTables = getDlmPolicyTables(sourceClusterId, sourceDataset, token);
				String sourceAtlasHost = httpGetObjectAmbari(sourceAmbariUrl+ambari_atlas_component_uri).getJSONArray("host_components").getJSONObject(0).getJSONObject("HostRoles").getString("host_name");
				String targetAtlasHost = httpGetObjectAmbari(targetAmbariUrl+ambari_atlas_component_uri).getJSONArray("host_components").getJSONObject(0).getJSONObject("HostRoles").getString("host_name");
				//String targetHiveServer = httpGetObjectAmbari(targetAmbariUrl+ambari_hive_component_uri).getJSONArray("host_components").getJSONObject(0).getJSONObject("HostRoles").getString("host_name");
				
				//String targetHiveServerUrl = "jdbc:hive2://" + targetHiveServer + ":" + hiveServerPort + "/" + targetDataset;
				//LOG.debug("********** Target Hive Url: " + targetHiveServerUrl);
				LOG.debug("********** Source Atlas Url: http://" + sourceAtlasHost + ":" + atlasPort);
				
				//replicateAtlasMetaData(sourceAtlasHost,sourceDataset,sourceCluster,targetCluster,targetAtlasHost,targetHiveServer,targetDataset,policyName,dlmPolicyTables);
				replicateAtlasMetaData(sourceAtlasHost,sourceDataset,sourceCluster,targetCluster,targetAtlasHost,targetDataset,policyName,dlmPolicyTables);
				//moveHiveTablesToCloudStorage(targetHiveServerUrl, targetDataset, dlmPolicyTables);
				
				Thread.sleep(5000);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	
		}
	}
	
	private static void printDSSEntityCollectionString(String token) throws JSONException {
		Map<String, Object> core = new HashMap<String, Object>();
		Map<String, Object> data = new HashMap<String, Object>();
		List<Object> dssCollections = getCollections(token);
		
		data.put("data", dssCollections);
		core.put("core", data);
		LOG.info("***************** " + core);
	}
	
	private static void printAtlasDebugInfo(AtlasClient sourceAtlasClient, 
			Referenceable database, 
			String sourceAtlasHost, 
			String sourceDataset, 
			String sourceCluster, 
			List<String> dlmPolicyTables) throws AtlasServiceException {
		
		Iterator<String> dlmReplicatedTablesIterator = dlmPolicyTables.iterator();
		while(dlmReplicatedTablesIterator.hasNext()) {
			String currTable = dlmReplicatedTablesIterator.next();				
			Referenceable table = searchEntity(sourceAtlasClient, "hive_table", sourceDataset + "." + currTable + "@" + sourceCluster);
			String orgTableId = table.getId()._getId();
			table = prepareCopyHiveTable(table, database.getId());
			LOG.debug("+++++++++++++ Exported Lineage: " + exportHiveTableLineageV2(sourceAtlasClient, getAtlasClientV2(sourceAtlasHost, atlasPort), orgTableId, table, database.getId()));
			LOG.debug("*************** " + getAtlasClient(sourceAtlasHost, atlasPort).getInputGraphForEntity(orgTableId));
			LOG.debug("*************** " + InstanceSerialization._toJson(getAtlasClientV2(sourceAtlasHost, atlasPort).getLineageInfo(orgTableId, LineageDirection.INPUT, 10), true));
		}
	}
	
	private static void replicateAtlasMetaData(String sourceAtlasHost, 
			String sourceDataset,
			String sourceCluster,
			String targetCluster,
			String targetAtlasHost, 
			//String targetHiveServer,
			String targetDataset,
			String policyName,
			List<String> dlmPolicyTables
			) throws AtlasServiceException{
		
		List<Referenceable> createEntitiesList = new ArrayList<Referenceable>();
		List<String> replicatedTablesList = new ArrayList<String>();
		
		AtlasClient sourceAtlasClient = getAtlasClient(sourceAtlasHost, atlasPort);
		AtlasClientV2 sourceAtlasClientV2 = getAtlasClientV2(sourceAtlasHost, atlasPort);
		AtlasClient targetAtlasClient = getAtlasClient(targetAtlasHost, atlasPort);
		AtlasClientV2 targetAtlasClientV2 = getAtlasClientV2(targetAtlasHost, atlasPort);
		
		Referenceable database = searchEntity(sourceAtlasClient, "hive_db",  sourceDataset + "@" + sourceCluster);
		database = resetEntityId("hive_db", database);
		
		Referenceable destinationDatabase = searchEntity(sourceAtlasClient, "hive_db",  sourceDataset + "@" + sourceCluster);
		destinationDatabase = createDestinationHiveDatabaseEntity(destinationDatabase, targetCluster);
		destinationDatabase = resetEntityId("hive_db", destinationDatabase);
		
		createEntitiesList.add(database);
		createEntitiesList.add(destinationDatabase);
		
		List<String> tagList = new ArrayList<String>();
		Iterator<String> dlmPolicyTablesIterator = dlmPolicyTables.iterator();
		while(dlmPolicyTablesIterator.hasNext()) {
			String currTable = dlmPolicyTablesIterator.next();				
			Referenceable table = searchEntity(sourceAtlasClient, "hive_table", sourceDataset + "." + currTable + "@" + sourceCluster);
			String orgTableId = table.getId()._getId();
			table = prepareCopyHiveTable(table, database.getId());
			//LOG.debug(getAtlasClient2(targetAtlasHost, atlasPort).getLineageInfo(orgTableId, "", 10));
			
			Map<String, AtlasEntity> entities = sourceAtlasClientV2.getEntityByGuid(orgTableId).getReferredEntities();
			for (AtlasEntity entity: entities.values()) {
				Iterator<AtlasClassification> classIter = entity.getClassifications().iterator();
				while(classIter.hasNext()) {
					AtlasClassification currClass = classIter.next();
					if(currClass != null)
						tagList.add(currClass.getTypeName());
				}
			}
			
			Referenceable destinationTable = searchEntity(sourceAtlasClient, "hive_table", sourceDataset + "." + currTable + "@" + sourceCluster);
			destinationTable = createDestinationHiveTableEntity(destinationDatabase, destinationTable, targetCluster);
			Referenceable dlmHiveProcess = createDlmHiveProcess(table, destinationTable, policyName, targetCluster);
			
			createEntitiesList.add(table);				
			//createEntitiesList.addAll(exportHiveTableLineage(sourceAtlasClient, orgTableId, table, database.getId()));
			createEntitiesList.addAll(exportHiveTableLineageV2(sourceAtlasClient, sourceAtlasClientV2, orgTableId, table, database.getId()));
			createEntitiesList.add(destinationTable);
			createEntitiesList.add(dlmHiveProcess);
		}
		
		SearchFilter searchFilter = new SearchFilter();
		searchFilter.setParam("type", "classification");
		//searchFilter.setParam("name", tagList);
		
		AtlasTypesDef sourceTypesDef = sourceAtlasClientV2.getAllTypeDefs(searchFilter);
		AtlasTypesDef targetTypesDef = targetAtlasClientV2.getAllTypeDefs(searchFilter);
		
		Set<String> sourceSuperTags = new HashSet<String>();
		List<AtlasClassificationDef> sourceClassDefList = sourceTypesDef.getClassificationDefs();
		Iterator<AtlasClassificationDef> sourceClassDefIter = sourceClassDefList.iterator();
		
		while (sourceClassDefIter.hasNext()) {
			AtlasClassificationDef sourceClass = sourceClassDefIter.next();
			String sourceClassName = sourceClass.getName();
			if(targetTypesDef.hasClassificationDef(sourceClassName)) {
				sourceClassDefIter.remove();
			}else {
				sourceSuperTags.addAll(sourceClass.getSuperTypes());
			}
		}
		
		AtlasTypesDef newTypesDef = new AtlasTypesDef();
		newTypesDef.setClassificationDefs(sourceClassDefList);
		targetAtlasClientV2.createAtlasTypeDefs(newTypesDef);
		
		/*
		for(int i=0; i < createEntitiesList.size(); i++) {
			LOG.info("*****************" + InstanceSerialization._toJson(createEntitiesList.get(i), true));
		}*/
		
		LOG.info("*****************" + InstanceSerialization._toJson(createEntitiesList, true));
		targetAtlasClient.createEntity(createEntitiesList);
	}
	
	private static void moveHiveTablesToCloudStorage(String targetHiveServerUrl, String targetDataset, List<String> dlmPolicyTables) throws ClassNotFoundException, SQLException {
		Connection hiveConnection = connectHive(targetHiveServerUrl, hiveUsername, hivePassword);
		showHiveTables(hiveConnection);
		
		Iterator<String> dlmReplicatedTablesIterator = dlmPolicyTables.iterator();
		while(dlmReplicatedTablesIterator.hasNext()) {
			String replicatedHiveTable = dlmReplicatedTablesIterator.next();
			createHiveTableAs(hiveConnection, targetDataset, replicatedHiveTable);
		}
	}
	
	private static List<Object> getCollections(String token) throws JSONException{
		List<Object> collectionList = new ArrayList<Object>();
		
		JSONArray collections = httpGetArray(dps_url+dss_collections_uri, token);
		LOG.debug("+++++++++++++ " + collections);
		for(int i=0; i < collections.length(); i++) {
			Map<String, Object> collectionMap = new HashMap<String, Object>();
			List<Object> assetList = new ArrayList<Object>();
			String collectionId = collections.getJSONObject(i).getJSONObject("dataset").getString("id");
			String collectionName = collections.getJSONObject(i).getJSONObject("dataset").getString("name");
			collectionMap.put("id", collectionId);
			collectionMap.put("text", collectionName);
		
			LOG.debug("+++++++++++++ Collection: " + collectionName);
			JSONArray assets = httpGetArray(dps_url+dss_assets_uri + "/" + collectionId + "/assets?queryName&offset=0&limit=100", token);
			for(int j = 0; j < assets.length(); j++) {
				Map<String, Object> asset = new HashMap<String, Object>();
				JSONObject assetJSON = assets.getJSONObject(j);
				String assetId = assetJSON.getString("id");
				String assetName = assetJSON.getString("assetName");
				String assetGuid = assetJSON.getString("guid");
				asset.put("id", assetGuid);
				asset.put("text", assetName);
				LOG.debug("+++++++++++++ AssetId: " + assetId + " AssetName: " + assetName + " GUID: " + assetGuid);
				assetList.add(asset);
			}
			collectionMap.put("children", assetList);
			collectionList.add(collectionMap);
		}
	
		return collectionList;
	}
	
	private static List<String> getDlmPolicyTables(String clusterId, String database, String token) throws JSONException {
		List<String> tables = new ArrayList<String>();
		String tablesUri = "/" + clusterId + "/hive/database/" + database + "/tables"; 
		LOG.debug("********** DLM Cluster Tables Url: " + dps_url+dlm_clusters_uri + tablesUri);
		JSONArray dbListJSON = httpGetObject(dps_url+dlm_clusters_uri + tablesUri, token).getJSONArray("dbList");
		LOG.debug("********** DLM Cluster Databases JSON: " + dbListJSON);
		
		for(int i = 0; i < dbListJSON.length(); i++) {				
			JSONObject currDbJSON = (JSONObject)dbListJSON.get(i);
			LOG.debug("********** DLM Database Entry: " + currDbJSON);
			
			for(int j = 0; j < currDbJSON.getJSONArray("table").length(); j++) {
				tables.add(currDbJSON.getJSONArray("table").getString(j));
			}
			break;
		}
		return tables;
	}

	private static Connection connectHive(String targetHiveServerUrl, String hiveUsername, String hivePassword) throws ClassNotFoundException, SQLException {
		Class.forName(hiveDriver);
		Connection hiveConnection = DriverManager.getConnection(targetHiveServerUrl, hiveUsername, hivePassword);
		
		return hiveConnection;
	}
	
	private static void showHiveTables(Connection hiveConnection) throws SQLException {
		ResultSet result = hiveConnection.createStatement().executeQuery("show tables");
		System.out.println("********** Tables: ");
		while(result.next()){
			System.out.println("table - " + result.getString(1));
		}
	}
	
	private static void createHiveTableAs(Connection hiveConnection, String database, String table) throws SQLException {
		String ddlStatement = "CREATE DATABASE IF NOT EXISTS " + warehouseDBName + " LOCATION '" + warehouseLocation + "'"; 
		hiveConnection.createStatement().execute(ddlStatement);
		ddlStatement = "CREATE TABLE " + warehouseDBName + "." + table + " AS SELECT * FROM " + database + "." + table;
	    LOG.debug(ddlStatement);
		hiveConnection.createStatement().execute(ddlStatement);
	}
	
	private static Map<String, JSONObject> getDpsClusters(String token) throws JSONException{
		Map<String, JSONObject> clusters = new HashMap<String, JSONObject>();
		
		JSONArray clustersJSON = httpGetArray(dps_url+dps_clusters_uri, token);
		LOG.debug("********** DPS Cluster JSON: " + clustersJSON);
		for(int i = 0; i < clustersJSON.length(); i++) {				
			JSONObject clusterJSON;
			clusterJSON = ((JSONObject)clustersJSON.get(i)).getJSONArray("clusters").getJSONObject(0);
			LOG.debug("********** Dps Cluster Entry: "+clusterJSON);
			clusters.put(clusterJSON.getString("name"), clusterJSON);
		}
		
		return clusters;
	}
	
	private static Map<String, JSONObject> getDlmClusters(String token) throws JSONException{
		Map<String, JSONObject> dlmClusters = new HashMap<String, JSONObject>();
		
		JSONArray dlmClustersJSON = httpGetObject(dps_url+dlm_clusters_uri, token).getJSONArray("clusters");
		LOG.debug("********** DLM Cluster JSON: " + dlmClustersJSON);
		for(int i = 0; i < dlmClustersJSON.length(); i++) {				
			JSONObject dlmClusterJSON = (JSONObject)dlmClustersJSON.get(i);
			LOG.debug("********** DLM Cluster Entry: "+dlmClusterJSON);
			dlmClusters.put(dlmClusterJSON.getString("name"), dlmClusterJSON);
		}
		
		return dlmClusters;
	}
	
	private static Map<String, JSONObject> getDlmPolicies(String token) throws JSONException{
		Map<String, JSONObject> dlmPolicies = new HashMap<String, JSONObject>();
		
		JSONArray dlmPoliciesJSON = httpGetObject(dps_url+dlm_policies_uri, token).getJSONArray("policies");
		LOG.debug("********** DLM Policies JSON: " + dlmPoliciesJSON);
		for(int i = 0; i < dlmPoliciesJSON.length(); i++) {				
			JSONObject dlmPolicyJSON = (JSONObject)dlmPoliciesJSON.get(i);
			LOG.debug("********** DLM Policy Entry: " + dlmPolicyJSON);
			dlmPolicies.put(dlmPolicyJSON.getString("name"), dlmPolicyJSON);
		}
		
		return dlmPolicies;
	}
	
	private static List<Referenceable> exportHiveTableLineage(AtlasClient atlasClient, String orgHiveTableGuid, Referenceable hiveTable, Id databaseId) {
		List<Referenceable> createEntitiesList = new ArrayList<Referenceable>();
		
		JSONObject tableLineage;
		try {
			tableLineage = atlasClient.getInputGraphForEntity(orgHiveTableGuid);
			JSONObject tableLineageEdges = tableLineage.getJSONObject("values").getJSONObject("edges");
			
			String currEdgeId = null;
			Referenceable currEntity = null;
			Referenceable prevEntity = hiveTable;
			
			for(int i = 0; i < tableLineageEdges.length(); i++) {
				if(i==0) {
					currEdgeId = tableLineageEdges.getJSONArray(orgHiveTableGuid).getString(0);
				}else {
					currEdgeId = tableLineageEdges.getJSONArray(currEdgeId).getString(0);
				}
				
				LOG.debug(currEdgeId);
				currEntity = atlasClient.getEntity(currEdgeId);
				if(currEntity.getTypeName().equalsIgnoreCase("hive_process")) {
					List<Id> in_outList = new ArrayList<Id>();
					
					currEntity = resetEntityId("hive_process", currEntity);
					Id prevEntityId = new Id(prevEntity.getId()._getId(), 0,"DataSet");
					in_outList.add(prevEntityId);
					currEntity.set("outputs",in_outList);
					createEntitiesList.add(currEntity);
				}else if(!(currEntity.getTypeName().equalsIgnoreCase("hive_process")) && prevEntity != null) {
					List<Id> in_outList = new ArrayList<Id>();
					
					if(currEntity.getTypeName().equalsIgnoreCase("hive_table")) {
						currEntity = prepareCopyHiveTable(currEntity, databaseId);
					} else if(currEntity.getTypeName().equalsIgnoreCase("hdfs_path")) {
						currEntity = resetEntityId("hdfs_path", currEntity);
					}
					Id currEntityId = new Id(currEntity.getId()._getId(), 0,"DataSet");
					in_outList.add(currEntityId);
					prevEntity.set("inputs", in_outList);
					createEntitiesList.add(currEntity);
				}
				prevEntity = currEntity;
				
			}
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return createEntitiesList;
	}
	
	private static List<Referenceable> exportHiveTableLineageV2(AtlasClient atlasClient, AtlasClientV2 atlasClientV2, String orgHiveTableGuid, Referenceable hiveTable, Id databaseId) {
		List<Referenceable> createEntitiesList = new ArrayList<Referenceable>();
		Map<String, String> relationMap = new HashMap<String, String>();
		AtlasLineageInfo tableLineage;
		
		try {
			tableLineage = atlasClientV2.getLineageInfo(orgHiveTableGuid, LineageDirection.INPUT, lineageDepth);
			LOG.debug("******* Lineage Info: " + tableLineage.getBaseEntityGuid());
			LOG.debug("******* Lineage Info: " + tableLineage.getGuidEntityMap());
			LOG.debug("******* Lineage Info: " + tableLineage.getRelations());
			
			Iterator<LineageRelation> lineage = tableLineage.getRelations().iterator();
			while(lineage.hasNext()) {
				LineageRelation relation= lineage.next();
				LOG.debug("----from: " + relation.getFromEntityId());
				LOG.debug("----to  : " + relation.getToEntityId());
				relationMap.put(relation.getToEntityId(), relation.getFromEntityId());
			}
			
			String currEdgeId = tableLineage.getBaseEntityGuid();
			Referenceable currEntity = null;
			Referenceable prevEntity = hiveTable;
			
			for(int i = 0; i < tableLineage.getRelations().size(); i++) {
				currEdgeId = relationMap.get(currEdgeId);
				
				LOG.debug(currEdgeId);
				currEntity = atlasClient.getEntity(currEdgeId);
				if(currEntity.getTypeName().equalsIgnoreCase("hive_process")) {
					List<Id> in_outList = new ArrayList<Id>();
					
					currEntity = resetEntityId("hive_process", currEntity);
					Id prevEntityId = new Id(prevEntity.getId()._getId(), 0,"DataSet");
					in_outList.add(prevEntityId);
					currEntity.set("outputs",in_outList);
					createEntitiesList.add(currEntity);
				}else if(!(currEntity.getTypeName().equalsIgnoreCase("hive_process")) && prevEntity != null) {
					List<Id> in_outList = new ArrayList<Id>();
					
					if(currEntity.getTypeName().equalsIgnoreCase("hive_table")) {
						currEntity = prepareCopyHiveTable(currEntity, databaseId);
					} else if(currEntity.getTypeName().equalsIgnoreCase("hdfs_path")) {
						currEntity = resetEntityId("hdfs_path", currEntity);
					}
					Id currEntityId = new Id(currEntity.getId()._getId(), 0,"DataSet");
					in_outList.add(currEntityId);
					prevEntity.set("inputs", in_outList);
					createEntitiesList.add(currEntity);
				}
				prevEntity = currEntity;
			}
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		} 
		
		return createEntitiesList;
	}
	
	private static Referenceable prepareCopyHiveTable(Referenceable hiveTable, Id database) {
		hiveTable = resetEntityId("hive_table", hiveTable);
		hiveTable.set("db", database.getId());

		List<Referenceable> columnList =  (List<Referenceable>) hiveTable.get("columns");
		columnList = resetNestedEntitiesId("hive_column", columnList, "table", hiveTable);
		hiveTable.set("columns", columnList);
		
		Referenceable sd = (Referenceable) hiveTable.get("sd");
		sd = resetEntityId("hive_table", sd);
		sd.set("table", hiveTable.getId());
		
		return hiveTable;
	}
	
	private static Referenceable createDlmHiveProcess(Referenceable inputHiveTable, Referenceable outputHiveTable, String policyName, String targetCluster) {
		String destinationTableFQN = outputHiveTable.get("qualifiedName").toString();
		String userName = outputHiveTable.get("owner").toString();
		String operationType = "Data Lifecycle Manager Replication";
		String startTime = outputHiveTable.get("lastAccessTime").toString();
		String endTime = outputHiveTable.get("lastAccessTime").toString();
		String queryId = "*";
		String queryText = "*";
		String queryPlan = "*";
		Referenceable dlmHiveProcess = new Referenceable("hive_process");
		
		List<Id> inList = new ArrayList<Id>();
		List<Id> outList = new ArrayList<Id>();
		
		Id inputHiveTableId = new Id(inputHiveTable.getId()._getId(), 0,"DataSet");
		inList.add(inputHiveTableId);
		
		Id outputHiveTableId = new Id(outputHiveTable.getId()._getId(), 0,"DataSet");
		outList.add(outputHiveTableId);
		
		dlmHiveProcess.set("name", policyName);
		dlmHiveProcess.set("clusterName", targetCluster);
		dlmHiveProcess.set("qualifiedName",  destinationTableFQN + ":" + policyName);
		dlmHiveProcess.set("userName", userName);
		dlmHiveProcess.set("operationType", operationType);
		dlmHiveProcess.set("startTime", startTime);
		dlmHiveProcess.set("endTime", endTime);
		dlmHiveProcess.set("queryId", queryId);
		dlmHiveProcess.set("queryText", queryText);
		dlmHiveProcess.set("queryPlan", queryPlan);
		dlmHiveProcess.set("inputs", inList);
		dlmHiveProcess.set("outputs", outList);
		
		return dlmHiveProcess;
	}
	
	private static Referenceable createDestinationHiveDatabaseEntity(Referenceable database, String targetCluster) {
		String databaseName = database.get("name").toString();
		Referenceable destinationDatabase = resetEntityId("hive_db", database);
		destinationDatabase.set("qualifiedName", databaseName + "@" + targetCluster);
		destinationDatabase.set("clusterName", targetCluster);
		
		return destinationDatabase;
	}
	
	private static Referenceable createDestinationHiveTableEntity(Referenceable database, Referenceable hiveTable, String targetCluster) {
		String databaseName = database.get("name").toString();
		String tableName = hiveTable.get("name").toString();
		Referenceable destinationHiveTable = hiveTable = resetEntityId("hive_table", hiveTable);
		
		destinationHiveTable.set("qualifiedName", databaseName + "." + tableName + "@" + targetCluster);
		destinationHiveTable.set("db", database.getId());
		
		List<Referenceable> columns = createDestinationColumns(database, destinationHiveTable, targetCluster); 
		destinationHiveTable.set("columns", columns);
		
		Referenceable sd = (Referenceable) destinationHiveTable.get("sd");
		sd = resetEntityId("hive_table", sd);
		sd.set("table", destinationHiveTable.getId());
		sd.set("qualifiedName", databaseName + "." + tableName + "@" + targetCluster + "_storage");
		destinationHiveTable.set("sd", sd);
		
		return destinationHiveTable;
	}
	
	private static List<Referenceable> createDestinationColumns(Referenceable database, Referenceable hiveTable, String targetCluster) {
		List<Referenceable> columns = (List<Referenceable>) hiveTable.get("columns");
		
		Iterator<Referenceable> columnsIterator = columns.iterator();
		while(columnsIterator.hasNext()) {
			Referenceable column = columnsIterator.next();
			column.replaceWithNewId(new Id("hive_columns"));
			column.set("table", hiveTable.getId());
			column.set("qualifiedName", database.get("name") + "." + hiveTable.get("name") + "." + column.get("name") + "@" + targetCluster);
		}
		
		return columns;
	}
	
	private static Referenceable searchEntity(AtlasClient atlasClient, String type, String entityFqn) {
		Referenceable entity = null;
		try {
			entity = atlasClient.getEntity(type, "qualifiedName", entityFqn);
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		}
		return entity;
	}
	
	private static Referenceable resetEntityId(String type, Referenceable entity) {
		Id newId = new Id(type);
		entity.replaceWithNewId(newId);
		
		return entity;
	}
	
	private static List<Referenceable> resetNestedEntitiesId(String type, List<Referenceable> entities, String parentField, Referenceable parentEntity) {
		Iterator<Referenceable> entityListIterator = entities.iterator();
		while(entityListIterator.hasNext()) {
			Referenceable entity = entityListIterator.next();
			entity.replaceWithNewId(new Id(type));
			entity.set(parentField, parentEntity.getId());
		}
		
		return entities;
	}
	
	private static AtlasClient getAtlasClient(String atlasHost, String atlasPort) {
		String atlasUrl = "http://"+atlasHost+":"+atlasPort;
		String[] atlasURL = {atlasUrl};
		AtlasClient atlasClient = new AtlasClient(atlasURL, atlasBasicAuth);
		
		return atlasClient;
	}
	
	private static AtlasClientV2 getAtlasClientV2(String atlasHost, String atlasPort) {
		String atlasUrl = "http://"+atlasHost+":"+atlasPort;
		String[] atlasURL = {atlasUrl};
		AtlasClientV2 atlasClient = new AtlasClientV2(atlasURL, atlasBasicAuth);
		
		return atlasClient;
	}
	
	private static JSONObject httpGetObject(String urlString, String token) {
    	    JSONObject response = null;
    	    String userpass = ambariAdminUser + ":" + ambariAdminPassword;
        String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
    		try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);            
            connection.setRequestProperty ("Authorization", basicAuth);
            connection.setRequestProperty  ("Cookie", token);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	response = new JSONObject(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
		return response;
    }
	
	private static JSONObject httpGetObjectAmbari(String urlString) {
	    JSONObject response = null;
	    String userpass = ambariAdminUser + ":" + ambariAdminPassword;
	    String basicAuth = "Basic " + new String(new Base64().encode(userpass.getBytes()));
	    InputStream content = null;
	    BufferedReader rd = null;
	    String jsonText = null;
	    
		URL url;
		try {
			url = new URL (urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			System.out.println(connection.getRequestProperties());
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);            
			connection.setRequestProperty ("Authorization", basicAuth);
			if (connection.getResponseCode() == 403) {
				LOG.info("********** Unauthorized, need SSO token");
				LOG.info(connection.getHeaderFields());
				content = (InputStream)connection.getErrorStream();
				rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
				jsonText = readAll(rd);
				response = new JSONObject(jsonText);
				
				LOG.info(response);
				LOG.info(response.getString("jwtProviderUrl")+url);
				httpGetObjectAmbari(response.getString("jwtProviderUrl")+url);
				System.exit(0);
			}if (connection.getResponseCode() == 307) {
				LOG.info("********** Redirect");
				LOG.info("********** " +connection.getHeaderField("Set-Cookie"));
			}else {
				LOG.info("********** Authorized");
				content = (InputStream)connection.getInputStream();
				rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
				jsonText = readAll(rd);
				response = new JSONObject(jsonText);
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return response;
	}
	
	private static JSONArray httpGetArray(String urlString, String token) {
    	JSONArray response = null;
    	try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Cookie", token);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	response = new JSONArray(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
		return response;
    }
	
	private JSONObject httpPostObject(String urlString, String payload, String token) {
    		JSONObject response = null;
    		try {
            URL url = new URL (urlString);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty  ("Cookie", token);
            connection.setRequestProperty("Content-Type", "application/json");
            OutputStream os = connection.getOutputStream();
    		os.write(payload.getBytes());
    		os.flush();
    		if (connection.getResponseCode() == 401) {
    			LOG.error("Token Rejected, refreshing...");
    			//String credentials = "{\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
    			//oAuthToken = getToken(cloudbreakAuthUrl, credentials);
    			//response = httpPostObject(urlString, payload);
    		}else if (connection.getResponseCode() > 202) {
    			throw new RuntimeException("Failed : HTTP error code : "+ connection.getResponseCode());
    		}else{
    			InputStream content = (InputStream)connection.getInputStream();
    			BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      		String jsonText = readAll(rd);
  	      		response = new JSONObject(jsonText);
    		}
        } catch(Exception e) {
            e.printStackTrace();
        }
		return response;
    }
	
	private static List<String> getToken(String urlString) {
		List<String> token = null;
		JSONObject response = null;
		String payload = "{\"username\":\""+dpsAdminUserName+"\",\"password\":\""+dpsAdminPassword+"\"}";
		try {
			URL url = new URL (urlString);
			HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setRequestMethod("POST");
			//connection.setRequestProperty  ("Authorization", "Bearer " + oAuthToken);
			connection.setRequestProperty("Content-Type", "application/json");
			OutputStream os = connection.getOutputStream();
			os.write(payload.getBytes());
			os.flush();
			
			if (connection.getResponseCode() == 401) {
				LOG.error("Token Rejected, refreshing...");
				//String credentials = "{\"username\":\""+adminUserName+"\",\"password\":\""+adminPassword+"\"}";
				//oAuthToken = getToken(cloudbreakAuthUrl, credentials);
				//response = httpPostObject(urlString, payload);
			}else if (connection.getResponseCode() > 202) {
				throw new RuntimeException("Failed : HTTP error code : "+ connection.getResponseCode());
			}else{
				InputStream content = (InputStream)connection.getInputStream();
				BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
	      		String jsonText = readAll(rd);
	      		response = new JSONObject(jsonText);
	      		token = connection.getHeaderFields().get("Set-Cookie");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return token;
	}
		
	private static String readAll(Reader rd) throws IOException {
		    StringBuilder sb = new StringBuilder();
		    int cp;
		    while ((cp = rd.read()) != -1) {
		      sb.append((char) cp);
		    }
		    return sb.toString();
	}
	
	private static void initializeTrustManager(){
		TrustManager[] trustAllCerts = new TrustManager[]{
			new X509TrustManager() {
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}
				public void checkClientTrusted(
					java.security.cert.X509Certificate[] certs, String authType) {
				}
				public void checkServerTrusted(
					java.security.cert.X509Certificate[] certs, String authType) {
				}
			}
		};

		//Install the all-trusting trust manager
		try {
			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
		    };
			
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
