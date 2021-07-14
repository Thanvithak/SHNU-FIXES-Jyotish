/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.sc2.talendForwardStandard;

import com.ads.mm.etl.xml.mapping.MMDBEnvironment;
import com.ads.mm.etl.xml.mapping.MMDBSystem;
import com.ads.mm.etl.xml.mapping.MMMapping;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author AmanSingh
 */
public class ConfigurationComponent {

    /* public static String addPreJobRepositoryComponents(Set<String> systemDBMSParticipatedInMapping, Map<String, String> contextEnvHostMap, Map<String, String> createContextMap, MMMapping mapping) {
        StringBuilder sb = new StringBuilder();
        int count = 1;
        int[] x = {64};
        final boolean[] isHiveComponentAvailable={false};
        for (Map.Entry<String, String> entry : contextEnvHostMap.entrySet()) {
            String host = entry.getKey();
            String port = entry.getValue();
            createContextMap.put("tHiveConfiguration_" + count + "_host", host);
            createContextMap.put("tHiveConfiguration_" + count + "_port", port);
            contextEnvHostMap.put(host, "tHiveConfiguration_" + count);
            count++;
        }
        systemDBMSParticipatedInMapping.forEach(dbmsSystem -> {
            String dbmsName = dbmsSystem.split(":")[0];
            dbmsSystem = dbmsSystem.replace("-", "_");
            x[0] = x[0] + 240 + dbmsSystem.length() * 2;
            String config = ConfigurationComponentEnum.valueOf(dbmsName).getConfigType();
            String dbVersion = ConfigurationComponentEnum.Oracle.name().equals(dbmsName) ? getOracleDbVersion(mapping, dbmsSystem) : "HDP_2_6";
            String component = "addComponent {\n"
                    + "	setComponentDefinition {\n"
                    + "		TYPE: \"" + config + "\",\n"
                    + "		NAME: \"" + dbmsSystem + "\",\n"
                    + "		POSITION: " + x[0] + ", 64,\n"
                    + "		SIZE: 32, 32,\n"
                    + "		OFFSETLABEL: 0, 0\n"
                    + "	}\n"
                    + "	setSettings {\n"
                    + "         DISTRIBUTION : \"HORTONWORKS\","
                    + "		DB_VERSION : \"" + dbVersion + "\",,\n"
                    + "		HOST : \"" + "context.tHiveConfiguration__host" + "\",\n"
                    + "		PORT :  \"" + "context.tHiveConfiguration__port" + "\",\n"
                    + "		USE_KRB : \"false\",\n"
                    + "		HIVE_PRINCIPAL : \"\\\"hive/_HOST@EXAMPLE.COM\\\"\",\n"
                    + "		USE_MAPRTICKET : \"false\",\n"
                    + "		SPARK_CONFIGURATION : \"tSparkConfiguration_1\",\n"
                    + "		CONNECTION_FORMAT : \"row\"\n"
                    + "	}\n"
                    + "}\n";
            sb.append(component);
            if (ConfigurationComponentEnum.Hortonworks.name().equalsIgnoreCase(dbmsName)) {
               isHiveComponentAvailable[0]=true;
            }
        });
        if(isHiveComponentAvailable[0]){
            sb.append(getHadoopComponent());
        }
         
        return sb.toString();
    }

    public static String getHiveComponent(String componetLabel, int count, int xPos) {
        StringBuilder sb = new StringBuilder();
        String component = "addComponent {\n"
                + "	setComponentDefinition {\n"
                + "		TYPE: \"tHiveConfiguration\",\n"
                + "		NAME: \" tHiveConfiguration_" + count + "\",\n"
                + "		POSITION: " + xPos + ", 64,\n"
                + "		SIZE: 32, 32,\n"
                + "		OFFSETLABEL: 0, 0\n"
                + "	}\n"
                + "	setSettings {\n"
                + "         DISTRIBUTION : \"HORTONWORKS\","
                + "		HOST : \"" + "context.tHiveConfiguration__host" + "\",\n"
                + "		PORT :  \"" + "context.tHiveConfiguration__port" + "\",\n"
                + "		USE_KRB : \"false\",\n"
                + "		HIVE_PRINCIPAL : \"\\\"hive/_HOST@EXAMPLE.COM\\\"\",\n"
                + "		USE_MAPRTICKET : \"false\",\n"
                + "		SPARK_CONFIGURATION : \"tSparkConfiguration_1\",\n"
                + "		CONNECTION_FORMAT : \"row\"\n"
                + "	}\n"
                + "}\n";
        sb.append(component);

        sb.append(getHadoopComponent());
        return sb.toString();
    }*/
    public static String addPreJobRepositoryComponents(Set<String> systemDBMSParticipatedInMapping, Map<String, String> contextEnvHostMap, Map<String, String> createContextMap, MMMapping mapping, final Map<String, String> configMap) {
        StringBuilder sb = new StringBuilder();
        int count = 1;
        int[] x = {64};
        final boolean[] isHiveComponentAvailable = {false};
        /*for (Map.Entry<String, String> entry : contextEnvHostMap.entrySet()) {
            String host = entry.getKey();
            String port = entry.getValue();
            createContextMap.put("tHiveConfiguration_" + count + "_host", host);
            createContextMap.put("tHiveConfiguration_" + count + "_port", port);
            contextEnvHostMap.put(host, "tHiveConfiguration_" + count);
            count++;
        }*/
        final int[] comCount = {1};
        systemDBMSParticipatedInMapping.forEach(dbmsSystem -> {

            String dbmsName = dbmsSystem.split(":")[0];
            ConfigurationComponentEnum dbType = ConfigurationComponentEnum.getConfigurationComponentEnum(dbmsName);
            if (dbType == null) {
                return;
            }
            String dbmsSystem1 = TalendUtilCat.hostIPParticipatedInMapping.get(dbmsSystem);
            // dbmsSystem = dbmsSystem.replace("-", "_");
            
            String component = "";
            String configComp=ConfigurationComponentEnum.valueOf(dbmsName).getConfigType() + "_" + comCount[0];
            switch (ConfigurationComponentEnum.valueOf(dbmsName)) {
                case Hortonworks:
                    x[0] = x[0] + 240 + dbmsSystem.length() * 2;
                    component = getHiveComponent(dbmsSystem1.replace("-", "_"), comCount[0], x[0]);
                    break;
                case Oracle:
                    x[0] = x[0] + 240 + dbmsSystem.length() * 2;
                    component = getOracleComponent(dbmsSystem1.replace("-", "_"), comCount[0], x[0], mapping, dbmsSystem);
                    break;
                case Snowflake:
                    x[0] = x[0] + 240 + dbmsSystem.length() * 2;
                    component = getSnowflakeComponent(dbmsSystem1.replace("-", "_"), comCount[0], x[0]);
                    break;
                case Teradata:
                    x[0] = x[0] + 240 + dbmsSystem.length() * 2;
                    component = getTeradataComponent(dbmsSystem1.replace("-", "_"), comCount[0], x[0]);
                    break;
                case SqlServer:
                case DB2:
                    x[0] = x[0] + 240 + dbmsSystem.length() * 2;
                    component = getJDBCComponent(dbmsSystem1.replace("-", "_"), comCount[0], x[0]);
                    break;
                case CSV:
                case DSV:
                case JSON:
                case XSD:
                    configComp="tHDFSConfiguration_1";
                    isHiveComponentAvailable[0] = true;
                    break;

            }
            configMap.put(dbmsSystem, configComp);
            sb.append(component);
            if (ConfigurationComponentEnum.Hortonworks.name().equalsIgnoreCase(dbmsName)) {
                isHiveComponentAvailable[0] = true;
               // x[0] = x[0] + 240;
                //sb.append(getHadoopComponent(x[0], comCount[0]));
            }
            comCount[0]++;

        });
         if (isHiveComponentAvailable[0]) {
             x[0] = x[0] + 240 ;
             sb.append(getHadoopComponent(x[0], 1));
        }

        return sb.toString();
    }

    public static String getHiveComponent(String componetLabel, int count, int xPos) {
        String sysEnv = componetLabel.substring(componetLabel.indexOf(":") + 1).replace(":", "_");
        String component = "addComponent {\n"
                + "	setComponentDefinition {\n"
                + "		TYPE: \"tHiveConfiguration\",\n"
                + "		NAME: \"tHiveConfiguration_" + count + "\",\n"
                + "		POSITION: " + xPos + ", 64,\n"
                + "		SIZE: 32, 32,\n"
                + "		OFFSETLABEL: 0, 0\n"
                + "	}\n"
                + "	setSettings {\n"
                + "		DISTRIBUTION : \"HORTONWORKS\",\n"
                + "		DB_VERSION : \"HDP_2_6\",,,\n"
                + "		ENABLE_HIVE_HA : \"false\",\n"
                + "		HOST : \"context." + sysEnv + "_Host\",\n"
                + "		PORT : \"context." + sysEnv + "_Port\",\n"
                + "		HIVE_METASTORE_URIS : \"\\\"thrift://host1:port1,thrift://host2:port2,...\\\"\",\n"
                + "		USE_KRB : \"false\",\n"
                + "		HIVE_PRINCIPAL : \"\\\"hive/_HOST@EXAMPLE.COM\\\"\",\n"
                + "		USE_MAPRTICKET : \"false\",\n"
                + "		SPARK_CONFIGURATION : \"tSparkConfiguration_1\",\n"
                + "		LABEL : \"tHiveConfiguration_" + count + "\",\n"
                + "		CONNECTION_FORMAT : \"row\"\n"
                + "	}\n"
                + "}";
        return component;
    }

    public static String getJDBCComponent(String componetLabel, int count, int xPos) {
        String sysEnv = componetLabel.substring(componetLabel.indexOf(":") + 1).replace(":", "_");
        String component = "addComponent {\n"
                + "	setComponentDefinition {\n"
                + "		TYPE: \"tJDBCConfiguration\",\n"
                + "		NAME: \"tJDBCConfiguration_" + count + "\",\n"
                + "		POSITION: " + xPos + ", 128,\n"
                + "		SIZE: 32, 32,\n"
                + "		OFFSETLABEL: 0, 0\n"
                + "	}\n"
                + "	setSettings {\n"
                + "		URL : \"context." + sysEnv + "_Url\",,\n"
                + "		DRIVER_CLASS : \"context." + sysEnv + "_Driver\",\n"
                + "		USER : \"context." + sysEnv + "_Username\",\n"
                + "		PASS : \"context." + sysEnv + "_Password\",\n"
                + "		ENCODING : \"\\\"ISO-8859-15\\\"\",\n"
                + "		ENCODING:ENCODING_TYPE : \"ISO-8859-15\",\n"
                + "		PROPERTIES : \"\\\"\\\"\",\n"
                + "		NOTE :\n"
                + "		\"*Note: Example for Additional JDBC Parameters:\n"
                + "				\\\"parameterName1=value1&&parameterName2=value2\\\"\",\n"
                + "		SPARK_CONFIGURATION : \"tSparkConfiguration_1\",\n"
                + "		POOL_MAX_TOTAL : \"8\",\n"
                + "		POOL_MAX_WAIT : \"-1\",\n"
                + "		POOL_MIN_IDLE : \"0\",\n"
                + "		POOL_MAX_IDLE : \"8\",\n"
                + "		POOL_USE_EVICTION : \"false\",\n"
                + "		POOL_TIME_BETWEEN_EVICTION : \"-1\",\n"
                + "		POOL_EVICTION_MIN_IDLE_TIME : \"1800000\",\n"
                + "		POOL_EVICTION_SOFT_MIN_IDLE_TIME : \"0\",\n"
                + "		CONNECTION_FORMAT : \"row\"\n"
                + "	}\n"
                + "}";

        return component;
    }

    public static String getSnowflakeComponent(String componetLabel, int count, int xPos) {
        String sysEnv = componetLabel.substring(componetLabel.indexOf(":") + 1).replace(":", "_");

        String component = "addComponent {\n"
                + "	setComponentDefinition {\n"
                + "		TYPE: \"tSnowflakeConfiguration\",\n"
                + "		NAME: \"tSnowflakeConfiguration_" + count + "\",\n"
                + "		POSITION: " + xPos + ", 64,\n"
                + "		SIZE: 32, 32,\n"
                + "		OFFSETLABEL: 0, 0\n"
                + "	}\n"
                + "	setSettings {\n"
                + "		NOTE :\n"
                + "		\"Warning! This component does not support running on Spark 1.3. Please choose one of the following Spark versions: 2.0, 2.1, 2.2, 2.3, 2.4\",\n"
                + "		NOTE :\n"
                + "		\"Warning! This component does not support running on Spark 1.4. Please choose one of the following Spark versions: 2.0, 2.1, 2.2, 2.3, 2.4\",\n"
                + "		NOTE :\n"
                + "		\"Warning! This component does not support running on Spark 1.5. Please choose one of the following Spark versions: 2.0, 2.1, 2.2, 2.3, 2.4\",\n"
                + "		NOTE :\n"
                + "		\"Warning! This component does not support running on Spark 1.6. Please choose one of the following Spark versions: 2.0, 2.1, 2.2, 2.3, 2.4\",\n"
                + "		ACCOUNT : \"context." + sysEnv + "_Account\",\n"
                + "		REGION : \"AWS_US_WEST\",\n"
                + "		USER : \"context." + sysEnv + "_Username\",\n"
                + "		PASS : \"context." + sysEnv + "_Password\",\n"
                + "		DBNAME : \"context." + sysEnv + "_Database\",\n"
                + "		DBSCHEMA : \"context." + sysEnv + "_Schema\",\n"
                + "		WAREHOUSE : \"context." + sysEnv + "_Warehouse\",\n"
                + "		SPARK_CONFIGURATION : \"tSparkConfiguration_1\",\n"
                + "		USE_CUSTOM_REGION : \"false\",\n"
                + "		CUSTOM_REGION : \"\\\"\\\"\",\n"
                + "		LABEL : \"tSnowflakeConfiguration_" + count + "\",\n"
                + "		CONNECTION_FORMAT : \"row\"\n"
                + "	}\n"
                + "}";
        return component;
    }

    public static String getOracleComponent(String componetLabel, int count, int xPos, MMMapping mapping, String dbmsSystem) {
        List<String> dbInfo = new ArrayList<>();

        String dbVersion = getOracleDbVersion(mapping, dbmsSystem, dbInfo);
        String component = "addComponent {\n"
                + "	setComponentDefinition {\n"
                + "		TYPE: \"tOracleConfiguration\",\n"
                + "		NAME: \"tOracleConfiguration_" + count + "\",\n"
                + "		POSITION: " + xPos + ", 64,\n"
                + "		SIZE: 32, 32,\n"
                + "		OFFSETLABEL: 0, 0\n"
                + "	}\n"
                + "	setSettings {\n"
                + "		CONNECTION_TYPE : \"ORACLE_SID\",\n"
                + "		DB_VERSION : \"" + dbVersion + "\",\n"
                + "		RAC_URL : \"\\\"\\\"\",\n"
                + "		USE_TNS_FILE : \"false\",\n"
                + "		TNS_FILE : \"\\\"\\\"\",,\n"
                + "		HOST : \"" + dbInfo.get(0) + "\",\n"
                + "		TYPE : \"Oracle\",\n"
                + "		PORT : \"" + dbInfo.get(4) + "\",\n"
                + "		DBNAME : \"\\\"" + dbInfo.get(1) + "\\\"\",\n"
                + "		LOCAL_SERVICE_NAME : \"\\\"\\\"\",\n"
                + "		SCHEMA_DB : \"\\\"\\\"\",\n"
                + "		USER : \"\\\"" + dbInfo.get(2) + "\\\"\",\n"
                + "		PASS : \"" + dbInfo.get(3) + "\",\n"
                + "		ENCODING : \"\\\"ISO-8859-15\\\"\",\n"
                + "		ENCODING:ENCODING_TYPE : \"ISO-8859-15\",\n"
                + "		PROPERTIES : \"\\\"\\\"\",\n"
                + "		NOTE :\n"
                + "		\"*Note: Example for Additional JDBC Parameters:\n"
                + "				\\\"parameterName1=value1&&parameterName2=value2\\\"\",\n"
                + "		JDBC_URL : \"\\\"jdbc:oracle:thin:USER/MDP@server\\\"\",\n"
                + "		POOL_MAX_TOTAL : \"8\",\n"
                + "		POOL_MAX_WAIT : \"-1\",\n"
                + "		POOL_MIN_IDLE : \"0\",\n"
                + "		POOL_MAX_IDLE : \"8\",\n"
                + "		POOL_USE_EVICTION : \"false\",\n"
                + "		POOL_TIME_BETWEEN_EVICTION : \"-1\",\n"
                + "		POOL_EVICTION_MIN_IDLE_TIME : \"1800000\",\n"
                + "		POOL_EVICTION_SOFT_MIN_IDLE_TIME : \"0\",\n"
                + "		LABEL : \"tOracleConfiguration_" + count + "\",\n"
                + "		CONNECTION_FORMAT : \"row\"\n"
                + "	}\n"
                + "}";

        return component;
    }

    public static String getTeradataComponent(String componetLabel, int count, int xPos) {
        String sysEnv = componetLabel.substring(componetLabel.indexOf(":") + 1).replace(":", "_");
        String component = "addComponent {\n"
                + "	setComponentDefinition {\n"
                + "		TYPE: \"tTeradataConfiguration\",\n"
                + "		NAME: \"tTeradataConfiguration_" + count + "\",\n"
                + "		POSITION: " + xPos + ", 64,\n"
                + "		SIZE: 32, 32,\n"
                + "		OFFSETLABEL: 0, 0\n"
                + "	}\n"
                + "	setSettings {\n"
                + "		HOST : \"context." + sysEnv + "_Host\",\n"
                + "		TYPE : \"Teradata\",\n"
                + "		DBNAME : \"context." + sysEnv + "_Database\",\n"
                + "		USER : \"context." + sysEnv + "_Username\",\n"
                + "		PASS : \"context." + sysEnv + "_Password\",\n"
                + "		PROPERTIES : \"\\\"\\\"\",\n"
                + "		ENCODING : \"\\\"ISO-8859-15\\\"\",\n"
                + "		ENCODING:ENCODING_TYPE : \"ISO-8859-15\",\n"
                + "		POOL_MAX_TOTAL : \"8\",\n"
                + "		POOL_MAX_WAIT : \"-1\",\n"
                + "		POOL_MIN_IDLE : \"0\",\n"
                + "		POOL_MAX_IDLE : \"8\",\n"
                + "		POOL_USE_EVICTION : \"false\",\n"
                + "		POOL_TIME_BETWEEN_EVICTION : \"-1\",\n"
                + "		POOL_EVICTION_MIN_IDLE_TIME : \"1800000\",\n"
                + "		POOL_EVICTION_SOFT_MIN_IDLE_TIME : \"0\",\n"
                + "		LABEL : \"tTeradataConfiguration_" + count + "\",\n"
                + "		CONNECTION_FORMAT : \"row\"\n"
                + "	}\n"
                + "}";
        return component;
    }

    private static String getCSVComponent(String componetLabel, int count, int xPos) {
        String hadoopComponent = "";
        try {
            hadoopComponent = "\n addComponent {\n"
                    + "	setComponentDefinition {\n"
                    + "		TYPE: \"tHDFSConfiguration\",\n"
                    + "		NAME: \"tHDFSConfiguration_" + count + "\",\n"
                    + "		POSITION: " + xPos + ", 64,\n"
                    + "		SIZE: 32, 32,\n"
                    + "		OFFSETLABEL: 0, 0\n"
                    + "	}\n"
                    + "	setSettings {\n"
                    + "         DISTRIBUTION : \"HORTONWORKS\","
                    + "		DB_VERSION : \"HDP_2_6\",,,\n"
                    + "		FS_DEFAULT_NAME : \"\\\"hdfs://localhost:9000/\\\"\",\n"
                    + "		USE_KRB : \"false\",\n"
                    + "		NAMENODE_PRINCIPAL : \"\\\"nn/_HOST@EXAMPLE.COM\\\"\",\n"
                    + "		USE_KEYTAB : \"false\",\n"
                    + "		PRINCIPAL : \"\\\"hdfs\\\"\",\n"
                    + "		KEYTAB_PATH : \"\\\"/tmp/hdfs.headless.keytab\\\"\",\n"
                    + "		USERNAME : \"\\\"\\\"\",\n"
                    + "		GROUP : \"\\\"supergroup\\\"\",\n"
                    + "		USE_DATANODE_HOSTNAME : \"true\",\n"
                    + "		TEMP_FOLDER : \"\\\"/tmp\\\"\",\n"
                    + "		USE_HDFS_ENCRYPTION : \"false\",\n"
                    + "		HDFS_ENCRYPTION_KEY_PROVIDER : \"\\\"kms://http@localhost:16000/kms\\\"\",,\n"
                    + "		LABEL : \"tHDFSConfiguration_" + count + "\",\n"
                    + "		CONNECTION_FORMAT : \"row\"\n"
                    + "	}\n"
                    + "}";
        } catch (Exception e) {
        }
        return hadoopComponent;
    }

    private static String getOracleDbVersion(MMMapping mapping, String dbmsSystem, List<String> dbInfo) {
        String dbVersion = null;
        boolean isVersionFound = false;
        final Object[] array;
        final Object[] systemArr = array = mapping.getSourceSystems().values().toArray();
        for (final Object systemArr2 : array) {
            final MMDBSystem system = (MMDBSystem) systemArr2;
            if (system != null) {
                final Object[] array2;
                final Object[] mmEnvironmentMap = array2 = system.getEnvironmentMap().values().toArray();
                for (final Object mmEnvironmentMap2 : array2) {
                    final MMDBEnvironment mmEnvironment = (MMDBEnvironment) mmEnvironmentMap2;
                    if (mmEnvironment == null) {
                        continue;
                    }
                    String dSystem = mmEnvironment.getDBMSType() + ":" + system.getSystemName() + ":" + mmEnvironment.getEnvironmentName();
                    if (!dbmsSystem.equals(dSystem)) {
                        continue;
                    }
                    final String hostName = mmEnvironment.getHostAddress();
                    dbInfo.add(hostName);//0
                    final String dbName = mmEnvironment.getDBMSName();
                    dbInfo.add(dbName);//1
                    final String user = mmEnvironment.getDbUserName();
                    dbInfo.add(user);//2
                    String pwd = mmEnvironment.getDbPassword();

                    pwd = DecryptUtility.decrypt(pwd);
                    dbInfo.add(pwd);//3
                    final String portNo = mmEnvironment.getHostPort();
                    dbInfo.add(portNo);//4
                    String versionInfo = DBConnection.getOracleDBVersion(user, pwd, "jdbc:oracle:thin:@" + hostName + ":" + portNo + ":" + dbName, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'", "oracle.jdbc.driver.OracleDriver", "banner");
                    dbVersion = findDbVersion(versionInfo);
                    dbInfo.add(dbVersion);//5
                    isVersionFound = true;
                    break;
                }
            }
            if (isVersionFound) {
                break;
            }
        }
        if (!isVersionFound) {
            final Object[] targetSystemArray = mapping.getTargetSystems().values().toArray();
            for (final Object systemArr2 : targetSystemArray) {
                final MMDBSystem system = (MMDBSystem) systemArr2;
                if (system != null) {
                    final Object[] array2;
                    final Object[] mmEnvironmentMap = array2 = system.getEnvironmentMap().values().toArray();
                    for (final Object mmEnvironmentMap2 : array2) {
                        final MMDBEnvironment mmEnvironment = (MMDBEnvironment) mmEnvironmentMap2;
                        if (mmEnvironment == null) {
                            continue;
                        }
                        String dSystem = mmEnvironment.getDBMSType() + ":" + system.getSystemName();
                        if (!dbmsSystem.equals(dSystem)) {
                            continue;
                        }
                        final String hostName = mmEnvironment.getHostAddress();
                        dbInfo.add(hostName);
                        final String dbName = mmEnvironment.getDBMSName();
                        dbInfo.add(dbName);
                        final String user = mmEnvironment.getDbUserName();
                        dbInfo.add(user);
                        String pwd = mmEnvironment.getDbPassword();
                        pwd = DecryptUtility.decrypt(pwd);
                        dbInfo.add(pwd);
                        final String portNo = mmEnvironment.getHostPort();
                        dbInfo.add(portNo);
                        String versionInfo = DBConnection.getOracleDBVersion(user, pwd, "jdbc:oracle:thin:@" + hostName + ":" + portNo + ":" + dbName, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'", "oracle.jdbc.driver.OracleDriver", "banner");
                        dbVersion = findDbVersion(versionInfo);
                        dbInfo.add(dbVersion);
                        isVersionFound = true;
                        break;
                    }
                }
                if (isVersionFound) {
                    break;
                }
            }
        }
        return dbVersion;
    }

    private static String getOracleDbVersion(MMMapping mapping, String dbmsSystem) {
        String dbVersion = null;
        boolean isVersionFound = false;
        final Object[] array;
        final Object[] systemArr = array = mapping.getSourceSystems().values().toArray();
        for (final Object systemArr2 : array) {
            final MMDBSystem system = (MMDBSystem) systemArr2;
            if (system != null) {
                final Object[] array2;
                final Object[] mmEnvironmentMap = array2 = system.getEnvironmentMap().values().toArray();
                for (final Object mmEnvironmentMap2 : array2) {
                    final MMDBEnvironment mmEnvironment = (MMDBEnvironment) mmEnvironmentMap2;
                    if (mmEnvironment == null) {
                        continue;
                    }
                    String dSystem = mmEnvironment.getDBMSType() + ":" + system.getSystemName() + ":" + mmEnvironment.getEnvironmentName();
                    if (!dbmsSystem.equals(dSystem)) {
                        continue;
                    }
                    final String hostName = mmEnvironment.getHostAddress();
                    final String dbName = mmEnvironment.getDBMSName();
                    final String user = mmEnvironment.getDbUserName();
                    String pwd = mmEnvironment.getDbPassword();
                    pwd = DecryptUtility.decrypt(pwd);
                    final String portNo = mmEnvironment.getHostPort();
                    String versionInfo = DBConnection.getOracleDBVersion(user, pwd, "jdbc:oracle:thin:@" + hostName + ":" + portNo + ":" + dbName, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'", "oracle.jdbc.driver.OracleDriver", "banner");
                    dbVersion = findDbVersion(versionInfo);
                    isVersionFound = true;
                    break;
                }
            }
            if (isVersionFound) {
                break;
            }
        }
        if (!isVersionFound) {
            final Object[] targetSystemArray = mapping.getTargetSystems().values().toArray();
            for (final Object systemArr2 : targetSystemArray) {
                final MMDBSystem system = (MMDBSystem) systemArr2;
                if (system != null) {
                    final Object[] array2;
                    final Object[] mmEnvironmentMap = array2 = system.getEnvironmentMap().values().toArray();
                    for (final Object mmEnvironmentMap2 : array2) {
                        final MMDBEnvironment mmEnvironment = (MMDBEnvironment) mmEnvironmentMap2;
                        if (mmEnvironment == null) {
                            continue;
                        }
                        String dSystem = mmEnvironment.getDBMSType() + ":" + system.getSystemName();
                        if (!dbmsSystem.equals(dSystem)) {
                            continue;
                        }
                        final String hostName = mmEnvironment.getHostAddress();
                        final String dbName = mmEnvironment.getDBMSName();
                        final String user = mmEnvironment.getDbUserName();
                        String pwd = mmEnvironment.getDbPassword();
                        pwd = DecryptUtility.decrypt(pwd);
                        final String portNo = mmEnvironment.getHostPort();
                        String versionInfo = DBConnection.getOracleDBVersion(user, pwd, "jdbc:oracle:thin:@" + hostName + ":" + portNo + ":" + dbName, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'", "oracle.jdbc.driver.OracleDriver", "banner");
                        dbVersion = findDbVersion(versionInfo);
                        isVersionFound = true;
                        break;
                    }
                }
                if (isVersionFound) {
                    break;
                }
            }
        }
        return dbVersion;
    }

    private static String findDbVersion(String versionInfo) {

        String defaultVersion = "ORACLE_11";
        String version = defaultVersion;
        if (StringUtils.isEmpty(versionInfo)) {
            version = defaultVersion;
        } else if (versionInfo.contains("Oracle Database 11")) {
            version = defaultVersion;
        } else if (versionInfo.contains("Oracle Database 12")) {
            version = "ORACLE_12";
        } else if (versionInfo.contains("Oracle Database 8")) {
            version = "ORACLE_8";
        } else if (versionInfo.contains("Oracle Database 9")) {
            version = "ORACLE_9";
        } else if (versionInfo.contains("Oracle Database 10")) {
            version = "ORACLE_10";
        }
        return version;
    }

    private static String getHadoopComponent(int xPos, int count) {
        String hadoopComponent = "";
        try {
            hadoopComponent = "\n addComponent {\n"
                    + "	setComponentDefinition {\n"
                    + "		TYPE: \"tHDFSConfiguration\",\n"
                    + "		NAME: \"tHDFSConfiguration_" + count + "\",\n"
                    + "		POSITION: " + xPos + ", 64,\n"
                    + "		SIZE: 32, 32,\n"
                    + "		OFFSETLABEL: 0, 0\n"
                    + "	}\n"
                    + "	setSettings {\n"
                    + "         DISTRIBUTION : \"HORTONWORKS\","
                    + "		DB_VERSION : \"HDP_2_6\",,,\n"
                    + "		FS_DEFAULT_NAME : \"\\\"hdfs://localhost:9000/\\\"\",\n"
                    + "		USE_KRB : \"false\",\n"
                    + "		NAMENODE_PRINCIPAL : \"\\\"nn/_HOST@EXAMPLE.COM\\\"\",\n"
                    + "		USE_KEYTAB : \"false\",\n"
                    + "		PRINCIPAL : \"\\\"hdfs\\\"\",\n"
                    + "		KEYTAB_PATH : \"\\\"/tmp/hdfs.headless.keytab\\\"\",\n"
                    + "		USERNAME : \"\\\"\\\"\",\n"
                    + "		GROUP : \"\\\"supergroup\\\"\",\n"
                    + "		USE_DATANODE_HOSTNAME : \"true\",\n"
                    + "		TEMP_FOLDER : \"\\\"/tmp\\\"\",\n"
                    + "		USE_HDFS_ENCRYPTION : \"false\",\n"
                    + "		HDFS_ENCRYPTION_KEY_PROVIDER : \"\\\"kms://http@localhost:16000/kms\\\"\",,\n"
                    + "		LABEL : \"tHDFSConfiguration_" + count + "\",\n"
                    + "		CONNECTION_FORMAT : \"row\"\n"
                    + "	}\n"
                    + "}";
        } catch (Exception e) {
        }
        return hadoopComponent;
    }

    private static String getHadoopComponent(int xPos) {
        String hadoopComponent = "";
        try {
            hadoopComponent = "\n addComponent {\n"
                    + "	setComponentDefinition {\n"
                    + "		TYPE: \"tHDFSConfiguration\",\n"
                    + "		NAME: \"tHDFSConfiguration_x\",\n"
                    + "		POSITION: " + xPos + ", 64,\n"
                    + "		SIZE: 32, 32,\n"
                    + "		OFFSETLABEL: 0, 0\n"
                    + "	}\n"
                    + "	setSettings {\n"
                    + "         DISTRIBUTION : \"HORTONWORKS\","
                    + "		DB_VERSION : \"HDP_2_6\",,,\n"
                    + "		FS_DEFAULT_NAME : \"\\\"hdfs://localhost:9000/\\\"\",\n"
                    + "		USE_KRB : \"false\",\n"
                    + "		NAMENODE_PRINCIPAL : \"\\\"nn/_HOST@EXAMPLE.COM\\\"\",\n"
                    + "		USE_KEYTAB : \"false\",\n"
                    + "		PRINCIPAL : \"\\\"hdfs\\\"\",\n"
                    + "		KEYTAB_PATH : \"\\\"/tmp/hdfs.headless.keytab\\\"\",\n"
                    + "		USERNAME : \"\\\"\\\"\",\n"
                    + "		GROUP : \"\\\"supergroup\\\"\",\n"
                    + "		USE_DATANODE_HOSTNAME : \"true\",\n"
                    + "		TEMP_FOLDER : \"\\\"/tmp\\\"\",\n"
                    + "		USE_HDFS_ENCRYPTION : \"false\",\n"
                    + "		HDFS_ENCRYPTION_KEY_PROVIDER : \"\\\"kms://http@localhost:16000/kms\\\"\",,\n"
                    + "		CONNECTION_FORMAT : \"row\"\n"
                    + "	}\n"
                    + "}";
        } catch (Exception e) {
        }
        return hadoopComponent;
    }

}
