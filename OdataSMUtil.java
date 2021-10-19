package com.erwin.metadata.odata;

import com.ads.api.beans.common.AuditHistory;
import com.ads.api.beans.sm.SMColumn;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.SystemManagerUtil;
import com.icc.util.RequestStatus;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public class OdataSMUtil {
    public static StringBuilder smLog = new StringBuilder();

    public static void main(String[] args) {
        Set<String> colSet = new LinkedHashSet<>();
    }

    public static int createSystem(SystemManagerUtil smUtil, String system) throws Exception {
        try {

            SMSystem smSystem = new SMSystem();
            smSystem.setSystemName(system);
            AuditHistory auditHistory = new AuditHistory();
            auditHistory.setCreatedBy("Administrator");
            smSystem.setAuditHistory(auditHistory);
            RequestStatus requestStatus = smUtil.createSystem(smSystem);
            smLog.append("\n" +system+" : "+ requestStatus.getStatusMessage());
            //System.out.println(requestStatus.getStatusMessage());

        } catch (Exception e) {
            e.printStackTrace();
//            smLog.append("\n" + e.getMessage());
        }
        return smUtil.getSystemId(system);
    }

    public static int createEnv(SystemManagerUtil smUtil, String system, String env, String instance, String schema) throws Exception {
        try {
            int systemId = smUtil.getSystemId(system);
            SMEnvironment sMEnvironment = new SMEnvironment();
            AuditHistory auditHistory = new AuditHistory();
            auditHistory.setCreatedBy("Administrator");
            sMEnvironment.setAuditHistory(auditHistory);
            sMEnvironment.setSystemEnvironmentName(env);
            sMEnvironment.setSystemEnvironmentType(env);
            sMEnvironment.setDatabaseType("SqlServer");
            sMEnvironment.setSystemId(systemId);
            sMEnvironment.setSystemName(system);
            sMEnvironment.setSystemEnvironmentType("OData");
            sMEnvironment.setServerPlatform("Windows");
            sMEnvironment.setServerOSVersion("X64");
            sMEnvironment.setDatabaseName(schema);
            sMEnvironment.setDatabaseURL("jdbc:sqlserver://OData:9999;databaseName=" + schema);
            sMEnvironment.setDatabaseDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            sMEnvironment.setDatabaseIPAddress(instance);
            sMEnvironment.setDatabasePort("9999");
            sMEnvironment.setDatabaseUserName("User Name");
            sMEnvironment.setDatabasePassword("Password");
            sMEnvironment.setDatabasePoolType("HIKARICP");
            sMEnvironment.setNoOfPartitions(2);
            //sMEnvironment.setVersion(env);
            sMEnvironment.setMinimumNoOfConnectionsPerPartition(3);
            sMEnvironment.setMaximumNoOfConnectionsPerPartition(5);
            RequestStatus requestStatus = smUtil.createEnvironment(sMEnvironment);


            smLog.append("\n" + env+" : "+requestStatus.getStatusMessage());
        } catch (Exception e) {
            e.printStackTrace();
//            smLog.append("\n" + e.getMessage());
        }
        return smUtil.getEnvironmentId(system, env);
    }

    public static String createMetadata(SystemManagerUtil smUtil, String system, String env, String schema, String tab, Set<String> colSet, String instance) {
        smLog = new StringBuilder();

        int systemId = 0;
        int envId = 0;
        int tableId = 0;
        try {
            try {
                systemId = smUtil.getSystemId(system);
            } catch (Exception e) {
                e.printStackTrace();
//                smLog.append("\n" + e.getMessage());
            }

            if (systemId < 1) {
                systemId = createSystem(smUtil, system);
            }


            try {
                envId = smUtil.getEnvironmentId(system, env);
            } catch (Exception e) {
                e.printStackTrace();
//                smLog.append("\n" + e.getMessage());
            }

            if (envId < 1) {
                envId = createEnv(smUtil, system, env, instance, schema);
                /*var envid= systemManagerUtil.getEnvironmentId(sysName, EnvName);
var msg=systemManagerUtil.versionEnvironment(envid, description, lable).getStatusMessage();
var tableList= systemManagerUtil.getEnvironmentTables(envid);
for(var i=0;i<tableList.size();
i++){

 

var table=tableList.get(i);
systemManagerUtil.deleteTable(table.getTableId());

 

}*/
            }

            tab = tab.trim();

            try {
                tableId = smUtil.getTableId(envId, schema + "." + tab);
            } catch (Exception e) {
                e.printStackTrace();
//                smLog.append("\n" + e.getMessage());
            }

            if (tableId < 1) {
                SMTable smTable = new SMTable();
                smTable.setEnvironmentId(envId);
                smTable.setTableName(tab);
                smTable.setSchemaName(schema);
                smTable.setTableType(SMTable.SMTableType.TABLE);
                smTable.setRootTable(true);
                smTable.setUsedInGapAnalysis(true);
                RequestStatus res = smUtil.createTable(smTable);
                smLog.append("\n" +tab+" : "+ res.getStatusMessage());
                tableId = smUtil.getTableId(envId, schema + "." + tab);
            }

            createCol(smUtil, tableId, colSet);


        } catch (Exception e) {
//            smLog.append("\n" + e.getMessage());
        }

        return smLog.toString();
    }

    public static void createCol(SystemManagerUtil smUtil, int tableId, Set<String> colSet) {

        try {
            Iterator value = colSet.iterator();

            while (value.hasNext()) {

                String colInfo = value.next().toString();

                String colName = colInfo.split("#@#")[0];
                String colDataType = colInfo.split("#@#")[1];
                String colNullable = colInfo.split("#@#")[2];
                String colDataLength = "";
                String colPrimaryKeyFlag = colInfo.split("#@#")[3];
                if (colInfo.split("#@#").length > 4) {
                    colDataLength = colInfo.split("#@#")[4];
                }

                int colId = 0;

                colId = smUtil.getColumnId(tableId, colName);
                if (colId == 0 || colId == -1) {
                    SMColumn smColumn = new SMColumn();
                    AuditHistory auditHistory = new AuditHistory();
                    auditHistory.setCreatedBy("Administrator");
                    smColumn.setAuditHistory(auditHistory);
                    smColumn.setTableId(tableId);
                    smColumn.setColumnName(colName);
                    smColumn.setColumnType(SMColumn.SMColumnType.ENTITY_ATTRIBUTE);
                    smColumn.setUsedInGapAnalysis(true);
                    smColumn.setColumnIdentityFlag(true);
                    smColumn.setValid(true);
                    smColumn.setPrimaryKeyFlag(Boolean.parseBoolean(colPrimaryKeyFlag));
                    smColumn.setColumnDatatype(colDataType);
                    smColumn.setColumnNullableFlag(Boolean.parseBoolean(colNullable));
                    smColumn.setColumnLength(colDataLength);
                    RequestStatus res = smUtil.createColumn(smColumn);

                    smLog.append("\n" +colName +" : "+res.getStatusMessage());
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
//            smLog.append("\n" + e.getMessage());
        }
    }
}
