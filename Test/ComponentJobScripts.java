/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.sc2.talendForwardStandard;

import com.ads.mm.etl.xml.mapping.MMDBEnvironment;
import java.util.HashMap;

/**
 *
 * @author ads
 */
public class ComponentJobScripts {

    public static HashMap<String, String> repoConnectionAginstEnv = new HashMap<String, String>();
    public static HashMap<String, String> repoStartConnectionInfo = new HashMap<String, String>();
    public static HashMap<String, String> repoCloseConnectionInfo = new HashMap<String, String>();

    public static String getConnections(HashMap<String, MMDBEnvironment> dbTypes) {
        StringBuilder sb = new StringBuilder();
        int i = 1;
        int x = 220;
        String source = "";
        for (String dbtype : dbTypes.keySet()) {
            if (!dbtype.equalsIgnoreCase("CSV") && !dbtype.equalsIgnoreCase("DSV")) {
                MMDBEnvironment environment = dbTypes.get(dbtype);

                String pwd = environment.getDbPassword();
                pwd = DecryptUtility.decrypt(pwd);

                String[] context = new String[]{environment.getHostAddress(),
                    environment.getHostPort(), environment.getDBMSName(), environment.getDBMSSchema(),
                    environment.getDbUserName(), pwd, "AdditionalParams"};
                String connName = dbtype + "_" + environment.getEnvironmentName();
                repoConnectionAginstEnv.put(environment.getEnvironmentName(), connName);

                if (i == 1) {
                    repoStartConnectionInfo.put(environment.getEnvironmentName(), "COMPONENT_OK" + "#" + "OnComponentOk" + "#" + "1" + "#" + "tPrejob_1" + "#" + "tPrejob_1" + "#" + connName);
                    source = connName;
                } else {
                    repoStartConnectionInfo.put(environment.getEnvironmentName(), "SUBJOB_OK" + "#" + "OnSubjobOk" + "#" + "1" + "#" + source + "#" + source + "#" + connName);
                    source = connName;
                }

//            tOracleConnection
                sb.append("addComponent {\n"
                        + " setComponentDefinition {\n"
                );
                if (dbtype.equalsIgnoreCase("oracle")) {
                    sb.append("TYPE: \"tOracleConnection\",\n");
                }
                if (dbtype.equalsIgnoreCase("sqlserver")) {
                    sb.append("TYPE: \"tMSSqlConnection\",\n");
                }
                sb.append("  NAME: \"" + connName + "\",\n"
                        + "  POSITION: " + x + "," + 32 + ",\n"
                        + "  SIZE: 32, 32,\n"
                        + "  OFFSETLABEL: 0, 0\n"
                        + " }\n");
                sb.append("setSettings {\n"
                        + "  HOST : \"" + "\"" + context[0] + "\"" + "\",\n"
                        + "  TYPE : \"" + "\"" + dbtype + "\"" + "\",\n"
                        + "  PORT : \"" + "\"" + context[1] + "\"" + "\",\n"
                        + "  DBNAME : \"" + "\"" + context[2] + "\"" + "\",\n"
                );
                if (dbtype.equalsIgnoreCase("oracle")) {

                    sb.append("  SCHEMA_DB : \"" + "\"" + context[3] + "\"" + "\",\n");
                } else {
                    sb.append("SCHEMA_DB :  \"\\\"dbo\\\"\",\n");
                }
                sb.append("  USER :  \"" + "\"" + context[4] + "\"" + "\",\n"
                        + "  PASS :  \"" + "\"" + context[5] + "\"" + "\",\n"
                        + "  PROPERTIES :  \"" + "\"" + context[6] + "\"" + "\",\n"
                        + "  ENCODING : \"\\\"ISO-8859-15\\\"\",\n"
                        + "  ENCODING:ENCODING_TYPE : \"ISO-8859-15\",\n"
                        + "  USE_SHARED_CONNECTION : \"false\",,\n"
                        + "  AUTO_COMMIT : \"false\",\n"
                        + "  CONNECTION_FORMAT : \"row\"\n"
                        + " }"
                        + "}"
                        + "\n");
            }
            i++;
            x = x + 220;
        }
        return sb.toString();
    }

    public static String getCloseConnections(HashMap<String, MMDBEnvironment> dbTypes) {
        StringBuilder sb = new StringBuilder();
        int i = 1;
        int x = 230;
        String source = "";
        for (String dbtype : dbTypes.keySet()) {
            if (!dbtype.equalsIgnoreCase("CSV") && !dbtype.equalsIgnoreCase("DSV")) {
                MMDBEnvironment environment = dbTypes.get(dbtype);
                String[] context = new String[]{environment.getHostAddress(),
                    environment.getHostPort(), environment.getDBMSName(), environment.getDBMSSchema(),
                    environment.getDbUserName(), environment.getDbPassword(), "AdditionalParams"};
                String componentType = TalendUtilCat.componentPropMap.get(dbtype).split(",")[0];
                String connName = dbtype + "_" + environment.getEnvironmentName();
                String connNameofRepo = repoConnectionAginstEnv.get(environment.getEnvironmentName());

                if (i == 1) {
                    repoCloseConnectionInfo.put(environment.getEnvironmentName(), "COMPONENT_OK" + "#" + "OnComponentOk" + "#" + "1" + "#" + "tPostjob_1" + "#" + "tPostjob_1" + "#" + connNameofRepo + "_Close");
                    source = connNameofRepo + "_Close";
                } else {
                    repoCloseConnectionInfo.put(environment.getEnvironmentName(), "SUBJOB_OK" + "#" + "OnSubjobOk" + "#" + "1" + "#" + source + "#" + source + "#" + connNameofRepo + "_Close");
                    source = connNameofRepo + "_Close";
                }

                sb.append("addComponent {\n"
                        + " setComponentDefinition {\n"
                );
                if (dbtype.equalsIgnoreCase("oracle")) {
                    sb.append("TYPE: \"tOracleClose\",\n");
                }
                if (dbtype.equalsIgnoreCase("sqlserver")) {
                    sb.append("TYPE: \"tMSSqlClose\",\n");
                }
                sb.append("  NAME: \"" + connName + "_Close\",\n"
                        + "  POSITION: " + x + "," + 384 + ",\n"
                        + "  SIZE: 32, 32,\n"
                        + "  OFFSETLABEL: 0, 0\n"
                        + " }\n");
                sb.append("setSettings {\n"
                        + "  CONNECTION : \"" + connNameofRepo + "\",\n"
                        + "  CONNECTION_FORMAT : \"row\"\n"
                        + " }"
                        + "}"
                        + "\n");
            }
            i++;
            x = x + 230;
        }
        return sb.toString();
    }
}
