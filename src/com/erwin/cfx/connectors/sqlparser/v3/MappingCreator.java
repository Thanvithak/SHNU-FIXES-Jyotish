/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.sqlparser.v3;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.mm.Project;
import com.ads.api.beans.mm.Subject;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.syncup.SyncMetadataJFDServerDBSchema;
import com.erwin.dataflow.model.xml.dataflow;
import com.erwin.dataflow.model.xml.table;
import com.erwin.sqlparser.util.CreateMappingVersion;
import com.erwin.sqlparser.wrapper.parser.ErwinSQLWrapper;
import com.erwin.util.XML2Model;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.icc.util.RequestStatus;
//import com.icc.util.RequestStatus;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TObjectName;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TUseDatabase;
import gudusoft.gsqlparser.stmt.mssql.TMssqlCreateProcedure;
import gudusoft.gsqlparser.*;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Balaji / Dinesh Arasankala
 */
public class MappingCreator {

    public static LinkedHashMap<String, HashSet<String>> keyValuesDeailsMap;

    public static LinkedHashMap<String, String> envMapWithTableNameAsKey = null;
    public static LinkedHashMap<String, String> envMapWithDataBaseNameAsKey = null;
    public static String useLine = "";
    public static final String DELIMITER = RelationAnalyzer.DELIMITER;// Fix T1
    public static KeyValueUtil kvUtil = null;
    public static int childSubjectId = 0;
    public static ArrayList<String> fileFoldersPathList = null;
    public static HashMap metadatChacheHM = new HashMap();
    public static HashMap<String, String> allTablesMap = new HashMap<String, String>();
    public static HashMap<String, String> allDBMap = new HashMap<String, String>();
    public static String defaultSystemName = "";
    public static String defaultEnviormentName = "";
    public static String defaultSchemaName = "";
    public static StringBuilder exceptionBuilder = new StringBuilder();
    public static int subjectCatOptionId = 0;
    public static String procedureName = "";//FIX T2 for getting procedure Name from GSP only
    public static Set<String> intermediateComponents= null;//FIX for getting IntermediateCOmponents Only

    // Added By Dinesh On June_22_2020
   public static void main(String[] args) {

//        String fileDirectoryPath = "E:\\Erwin\\Files\\SQLParsing\\Exeter\\SQLParserFinal_070520\\Files\\sqls\\TestInput";
        String fileDirectoryPath = "D:\\Temp sqlparser";
//        String fileDirectoryPath = "C:\\Users\\Balaji\\Desktop\\SQL Parser\\Exeter\\InputFiles\\Mapping Level Errors";
//                + "\\GS_AntiMalwareHealthStatusLoad_output";

        //  String testPath = "D:\\Dinesh Code Base\\25SSIS REV ENGG_Puma\\23SQLEnhance\\SqlTest\\SqlFolder1\\alalis.sql";
        // String mapName = testPath.substring(testPath.lastIndexOf("\\")+1);
        String[] sysEnvDetails = {"SrcSystem", "SrcEnv", "TgtSystem", "TgtEnv"};
        File dir = new File(fileDirectoryPath);

        dir = new File(fileDirectoryPath);

        search(".*\\.sql", dir, sysEnvDetails);
        //     callSubDirctory(dir, childSubjectId, childSubjectId, new MappingManagerUtil("auth"));

        HashMap inputpropertiesMap = new HashMap();
        inputpropertiesMap.put("fileDirectory", "C:\\Users\\VinithChalla\\Desktop\\SnhuFix");
        inputpropertiesMap.put("projectId", 0);
        inputpropertiesMap.put("maputil", null);
        inputpropertiesMap.put("systemManagereUtil", null);
        inputpropertiesMap.put("databaseType", "msssql");
        inputpropertiesMap.put("keyValueUtil", null);
        inputpropertiesMap.put("jsonPath", "C:\\erwin\\Metadata\\");
        inputpropertiesMap.put("defaultSchemaName", "DBO");
        inputpropertiesMap.put("loadTypeFromCat", "full");
        inputpropertiesMap.put("executionTimeDate", "0000");
        inputpropertiesMap.put("syncUpFailedLogs", "C:\\erwin\\Log\\");
        inputpropertiesMap.put("deleteSourceFileType", "None");
        inputpropertiesMap.put("subjectName", "");
        inputpropertiesMap.put("archivePath", "C:\\erwin\\archive");
        MappingCreator creator = new MappingCreator();
        //creator.getMappingObjectToJsonForSql(sysEnvDetails, inputpropertiesMap);

    }
     public static void search(String pattern, File folder, String[] sysEnvDetails) {
        for (File f : folder.listFiles()) {
            if (f.isDirectory()) {
                search(pattern, f, sysEnvDetails);
            }
            if (f.isFile()) {
                if (f.getName().matches(pattern)) {
                    System.out.println("------------- FileName is--->" + f.getAbsolutePath() + "<----------------");
//                    MappingCreator_BeforeDeletingSourceFiles mappingCreator = new MappingCreator_BeforeDeletingSourceFiles();
//                    mappingCreator.getMappingSpecList(f.getAbsolutePath(), sysEnvDetails);
                    MappingCreator mappingCreator = new MappingCreator();
                    mappingCreator.getMappingSpecList(f.getAbsolutePath(), sysEnvDetails);
                }
            }

        }
    }
             public ArrayList<MappingSpecificationRow> getMappingSpecList(String inputFilePath, String[] sysenvDetails) {
        try {
            Set<String> storeProceduresSet = ErwinSQLWrapper.getAllStatementsForSqlFile(inputFilePath, EDbVendor.dbvmssql);
            //File inputFile = new File(inputFilePath);
            //String fileName = getFileName(inputFilePath);
            String fileName = "Twes";
            //String sqltext = FileUtils.readFileToString(inputFile, "UTF-8");
            for(String sqltext : storeProceduresSet){
                 sqltext = ErwinSQLWrapper.removeUnparsedDataFromQuery(sqltext);
            dataflow dtflow = getDataflowFromSql(sqltext, fileName, "mssql");
            if (dtflow == null) {
               // return new ArrayList<>();
               continue;
            }
            RelationAnalyzer relationAnalyzer = new RelationAnalyzer();

//            ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails, this.tableSystemEnvMap, this.metadataTableColumnDetailsMap);
            ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
            this.keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();

            for (MappingSpecificationRow mspecRow : mapSpecRows) {
                System.out.println(mspecRow.getSourceTableName() + "====>" + mspecRow.getSourceColumnName() + "====>"
                        + mspecRow.getBusinessRule() + "===>" + mspecRow.getTargetTableName() + "====>" + mspecRow.getTargetColumnName());
            }
            System.out.println("keyValuesMap ===> " + this.keyValuesDeailsMap);

           // return mapSpecRows;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public String getMappingObjectToJsonForSql(String[] sysenvDetails, HashMap inputpropertiesMap) {
        Mapping mapping = null;
        String mappingJson = "";
        String completeStatus = "";
        String status = "";
        String filePathFromCat = "";
        int projectId = 0;
        MappingManagerUtil mappingManagerUtil = null;
        SystemManagerUtil systemManagerUtil = null;
        String databaseType = "";
        KeyValueUtil keyValueUtil = null;
        String jsonFilePath = "";
        String defaultSchemaNameFromCat = "";
        String loadType = "";
        String executionDateTime = "";
        subjectCatOptionId = 0;

        String logFilePath = "";
        String deleteOrArchiveSourceFile = "";
        String subjectNameFromCat = "";
        String archivePath = "";

        // prepare input data from cat into local varibles
        filePathFromCat = (String) inputpropertiesMap.get("fileDirectory");
        projectId = Integer.parseInt(inputpropertiesMap.get("projectId").toString());
        mappingManagerUtil = (MappingManagerUtil) inputpropertiesMap.get("maputil");
        systemManagerUtil = (SystemManagerUtil) inputpropertiesMap.get("systemManagereUtil");
        databaseType = (String) inputpropertiesMap.get("databaseType");
        keyValueUtil = (KeyValueUtil) inputpropertiesMap.get("keyValueUtil");
        jsonFilePath = (String) inputpropertiesMap.get("jsonPath");
        defaultSchemaNameFromCat = (String) inputpropertiesMap.get("defaultSchemaName");
        loadType = (String) inputpropertiesMap.get("loadTypeFromCat");
        executionDateTime = (String) inputpropertiesMap.get("executionTimeDate");
        logFilePath = (String) inputpropertiesMap.get("syncUpFailedLogs");
        deleteOrArchiveSourceFile = (String) inputpropertiesMap.get("deleteSourceFileType");
        subjectNameFromCat = (String) inputpropertiesMap.get("subjectName");
        String logHeaderWithCATParam = (String) inputpropertiesMap.get("logHeaderWithCATParam");
        String logFileName = "SQL Parser log_";
        String fileExtension = "log";

        if (!StringUtils.isBlank(subjectNameFromCat)) {
            subjectNameFromCat = FilenameUtils.normalize(subjectNameFromCat, true);//FIX T3 for Replacing backward with forward
        }

        archivePath = (String) inputpropertiesMap.get("archivePath");
        // completed of preparing input data from the cat

        kvUtil = keyValueUtil;
        fileFoldersPathList = new ArrayList();
        int subjectId = 0;
        childSubjectId = 0;
        ObjectMapper objectMapper = new ObjectMapper();

        jsonFilePath = FilenameUtils.normalizeNoEndSeparator(jsonFilePath, true) + "/";//FIX T3 for Replacing backward with forward
        archivePath = FilenameUtils.normalizeNoEndSeparator(archivePath, true) + "/";//FIX T3 for Replacing backward with forward
        logFilePath = FilenameUtils.normalize(logFilePath, true);//FIX T3 for Replacing backward with forward
        filePathFromCat = FilenameUtils.normalize(filePathFromCat, true);//FIX T3 for Replacing backward with forward

        try {
            createFile(logFilePath, executionDateTime, logHeaderWithCATParam, "logData", logFileName, fileExtension);
        } catch (Exception e) {

        }

        allDBMap = SyncMetadataJFDServerDBSchema.getMap(jsonFilePath, "Databases");// FIX T4 as we chnaged the Type by adding extra parameter
//        allTablesMap = SyncMetadataJFDServerDBSchema.getMap(jsonFilePath, "Tables");// FIX T4 as we chnaged the Type by adding extra parameter
        metadatChacheHM = new HashMap();
        try {

            File inputFile = null;
            inputFile = new File(filePathFromCat);
            String mapName = "";

            if (!StringUtils.isBlank(subjectNameFromCat)) {
                String[] subjectArraySpilt = null;
                if (subjectNameFromCat.contains(",")) {
                    subjectArraySpilt = subjectNameFromCat.split(",");
                } else {
                    subjectArraySpilt = subjectNameFromCat.split("/");
                }

                int subjectCount = 0;

                if (subjectArraySpilt != null) {
                    for (String subjectHierarchy : subjectArraySpilt) {
                        if (subjectCount == 0 && !StringUtils.isBlank(subjectHierarchy)) {
                            subjectId = createSubject(subjectHierarchy, projectId, mappingManagerUtil);
                        } else {
                            if (!StringUtils.isBlank(subjectHierarchy)) {
                                subjectId = createChildSubject(subjectHierarchy, projectId, subjectId, mappingManagerUtil);
                            }
                        }
                        if (!StringUtils.isBlank(subjectHierarchy)) {
                            subjectCount++;
                        }
                    }
                }
                subjectCatOptionId = subjectId;
            }

            File[] sqlfilearr = inputFile.listFiles();
            for (File sqlFile : sqlfilearr) {

                if ((filePathFromCat.contains("vUpload") || sqlFile.isDirectory())) {
                    String subjectName = sqlFile.getName();
                    try {
                        if (subjectName.contains(".")) {
                            subjectName = subjectName.substring(0, subjectName.lastIndexOf("."));
                            subjectName = subjectName.replace(".", "_");
                        }
                        subjectName = subjectName.replaceAll("[^a-zA-Z0-9 _-]", "_");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (StringUtils.isBlank(subjectNameFromCat)) {
                        subjectId = createSubject(subjectName, projectId, mappingManagerUtil);
                    } else {
                        subjectId = createChildSubject(subjectName, projectId, subjectCatOptionId, mappingManagerUtil);
                    }
                }

                if (sqlFile.isDirectory()) {
                    callSubDirctory(sqlFile, subjectId, projectId, mappingManagerUtil, deleteOrArchiveSourceFile, filePathFromCat, subjectNameFromCat);
                } else if (!StringUtils.isBlank(subjectNameFromCat)) {
                    fileFoldersPathList.add(sqlFile.getAbsolutePath() + DELIMITER + subjectId);//FIX T5 Adding Delimiter 
                } else {
                    if (!filePathFromCat.contains("vUpload")) {
                        subjectId = -1;
                    }
                    fileFoldersPathList.add(sqlFile.getAbsolutePath() + DELIMITER + subjectId);//FIX T5 Adding Delimiter
                }
            }

            String folderServerName = "";
            String folderDataBaseName = "";
            defaultSystemName = sysenvDetails[0];
            defaultEnviormentName = sysenvDetails[1];
            defaultSchemaName = defaultSchemaNameFromCat;
            String fileName = "";
            String schemaNameFromFolder = "";

            if (fileFoldersPathList.isEmpty()) {
                completeStatus = "No files are thier in path to create mappings";
            }

            List<String> mapNamesList = new ArrayList();
            for (String subFilePath : fileFoldersPathList) {
                exceptionBuilder = new StringBuilder();
                int i = 0;
                boolean flag = false;

                StringBuilder outerExceptionBuilder = new StringBuilder();
                String filePath = "";
                try {

                    if (subFilePath.contains(DELIMITER)) {//FIX T5 Adding Delimiter
                        int length = subFilePath.split(DELIMITER).length;//FIX T5 Adding Delimiter
                        if (length >= 1) {
                            filePath = subFilePath.split(DELIMITER)[0];//FIX T5 Adding Delimiter
                            String subjectStringId = subFilePath.split(DELIMITER)[1];//FIX T5 Adding Delimiter
                            subjectStringId = subjectStringId.replace("\"", "");
                            subjectId = Integer.parseInt(subjectStringId);
                            filePath = FilenameUtils.normalize(filePath, true);//FIX T3 for Replacing backward with forward

                            mapName = FilenameUtils.getBaseName(filePath);// FIX T8 using FilenameUtilsfor getting fileName only
                            fileName = FilenameUtils.getBaseName(filePath);// FIX T8 using FilenameUtilsfor getting fileName only
                        }
                    }
                } catch (Exception e) {
                    outerExceptionBuilder.append("Exception Details: " + e + "\n");
                }

                Set<String> storeProceduresSet = null;
                int exceptionCount = 0;
                try {
                    String[] extensions = {"sql", "txt"};// FIX T6 checking Extensions using FileNameUtils
                    if (FilenameUtils.isExtension(filePath.toLowerCase(), extensions)) {//FIX T6 checking Extensions using FileNameUtils
                        EDbVendor dbVendor = getDBVendorFromStringVendorName(databaseType);
                        storeProceduresSet = ErwinSQLWrapper.getAllStatementsForSqlFile(filePath, dbVendor);
                    } else {
                        String execeptionDeatils = "{\"Output\":{\"statusDescription\":\"Error\",\"statusNumber\":0,\"requestSuccess\":true,\"userObject\":null,\"id\":null,\"statusMessage\":\"Not able to parse the Query\"," + "\n";
                        execeptionDeatils = execeptionDeatils + "\"FileName \" :" + "\"" + fileName + "\"" + ",\n";
                        execeptionDeatils = execeptionDeatils + "\"FilePath \" :" + "\"" + filePath + "\"" + ",\n";
                        execeptionDeatils = execeptionDeatils + "\"MapName \" :" + "\"" + "null" + "\"" + "}\n}";
                        outerExceptionBuilder.append(execeptionDeatils);
                        execeptionDeatils = "";
                        flag = true;
                    }

                } catch (Exception e) {
                    storeProceduresSet = null;
                    outerExceptionBuilder.append("Exception Details: " + e + "\n");
                }

                try {
                    if (!filePath.contains("vUpload")) {
                        filePath = FilenameUtils.normalizeNoEndSeparator(filePath, true); //FIX T3 for Replacing backward with forward
                        String filePathSpilt[] = filePath.split("/");
                        if (filePathSpilt.length > 4) {
                            schemaNameFromFolder = filePathSpilt[filePathSpilt.length - 2];
                            folderDataBaseName = filePathSpilt[filePathSpilt.length - 3];
                            folderServerName = filePathSpilt[filePathSpilt.length - 4];
                        }
                    }
                } catch (Exception e) {
                    outerExceptionBuilder.append("Exception Details: " + e + "\n");
                }

                if (storeProceduresSet != null) {
                    useLine = "";
                    for (String sqltext : storeProceduresSet) {// FIX T7 changing iterator with for loop
                        procedureName = "";
                        exceptionBuilder = new StringBuilder().append(outerExceptionBuilder.toString());
                        status = "";

                        mapName = FilenameUtils.getBaseName(filePath);// FIX T8 using FilenameUtilsfor getting fileName only
                        dataflow dtflow = getDataflowFromSql(sqltext, mapName, databaseType);

                        if (!StringUtils.isBlank(procedureName)) {// FIX T9 for Getting ProcedureName
                            mapName = procedureName.replace(".", "_").replace("]", "").replace("[", "");
                        }
                        mapName = mapName.replaceAll("[^a-zA-Z0-9 _-]", "_");

                        ArrayList<MappingSpecificationRow> mapSpecRows = null;
                        if (dtflow != null) {
                            RelationAnalyzer relationAnalyzer = new RelationAnalyzer();
                            mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
                            keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();
                        }

                        if (dtflow == null || mapSpecRows == null || mapSpecRows.isEmpty()) {// FIX T10 for dataflow returns Null Object
                            String execeptionDeatils = "{\"Output\":{\"statusDescription\":\"Error\",\"statusNumber\":0,\"requestSuccess\":true,\"userObject\":null,\"id\":null,\"statusMessage\":\"Not able to parse the Query\"," + "\n";
                            execeptionDeatils = execeptionDeatils + "\"FileName \" :" + "\"" + fileName + "\"" + ",\n";
                            execeptionDeatils = execeptionDeatils + "\"FilePath \" :" + "\"" + filePath + "\"" + ",\n";
                            execeptionDeatils = execeptionDeatils + "\"MapName \" :" + "\"" + "null" + "\"" + "}\n}";
                            exceptionBuilder.append(execeptionDeatils);
                            execeptionDeatils = "";
                            exceptionCount++;
                        }
                        if (mapNamesList.contains(mapName + subjectId)) {
                            mapName = mapName + "_" + ++i;
                        }
                        mapNamesList.add(mapName + subjectId);

                        if (mapSpecRows != null && mapSpecRows.size() > 0) {
                            mapping = new Mapping();
                            mapping.setMappingName(mapName);
                            mapping.setProjectId(projectId);
                            mapping.setSubjectId(subjectId);
                            mapping.setMappingSpecifications(mapSpecRows);
                            mapping.setSourceExtractQuery(sqltext);

                            mappingJson = objectMapper.writeValueAsString(mapping);

                            if (mappingJson != null || !mappingJson.isEmpty()) {
                                status = createMappingFromJson(mappingJson, mapName, subjectId, projectId, mappingManagerUtil, jsonFilePath, folderServerName, folderDataBaseName, schemaNameFromFolder, fileName, loadType, executionDateTime, logFilePath, filePath, sqltext, deleteOrArchiveSourceFile, filePathFromCat, archivePath) + "\n";
                            }
                        }

                        completeStatus = completeStatus + " " + status + "\n";

                        createFile(logFilePath, executionDateTime, status, "logData", logFileName, fileExtension);
                    }
                }

                try {
                    int storeProcSize = 0;
                    if (storeProceduresSet != null) {
                        storeProcSize = storeProceduresSet.size();
                    }

                    String individualStatus = "";
                    individualStatus = writeExeceptionDeatilsIntoFile(flag, storeProcSize, exceptionCount, outerExceptionBuilder, exceptionBuilder, logFilePath, executionDateTime);

                    completeStatus = completeStatus + " " + individualStatus + "\n";
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            exceptionBuilder.append("Exception Details: " + ex + "\n");
        }
        return completeStatus;
    }

    public static void callSubDirctory(File directories, int parentSubjectId, int projectId, MappingManagerUtil mappingManagerUtil, String deleteOrArchiveSourceFile, String filePathFromCat, String subjectNameFromCat) {

        File[] subDirectories = directories.listFiles();
        int length = subDirectories.length;
        int count = 0;

        for (File subFile : subDirectories) {
            String subjectName = subFile.getName();
            count++;

            try {

                if (subFile.isDirectory()) {

                    try {

//                        if (StringUtils.isBlank(subjectNameFromCat)) {
                        if (subjectName.contains(".")) {
                            subjectName = subjectName.substring(0, subjectName.lastIndexOf("."));
                            subjectName = subjectName.replace(".", "_");

                        }

                        subjectName = subjectName.replaceAll("[^a-zA-Z0-9 _-]", "_");
                        childSubjectId = createChildSubject(subjectName, projectId, parentSubjectId, mappingManagerUtil);
//                        }

                    } catch (Exception e) {

                        e.printStackTrace();

                    }
                    if (count >= length) {
                        parentSubjectId = childSubjectId;
                    }

                    callSubDirctory(subFile, childSubjectId, projectId, mappingManagerUtil, deleteOrArchiveSourceFile, filePathFromCat, subjectNameFromCat);
                } else {
                    String filePathAndSubjectId = "";
//                    if (StringUtils.isBlank(subjectNameFromCat)) {
                    filePathAndSubjectId = subFile.getAbsolutePath() + DELIMITER + parentSubjectId;//FIX T5 Adding Delimiter
//                    }
//                    else {
//                        filePathAndSubjectId = subFile.getAbsolutePath() + DELIMITER + subjectCatOptionId;//FIX T5 Adding Delimiter
//                    }

                    fileFoldersPathList.add(filePathAndSubjectId);

                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void archiveSourceFiles(String filePath, String deleteOrArchiveSourceFile, String filePathFromCat, String archivePathFromCat) {

        filePath = FilenameUtils.normalize(filePath, true);//FIX T3 for Replacing backward with forward
        filePathFromCat = FilenameUtils.normalizeNoEndSeparator(filePathFromCat, true);//FIX T3 for Replacing backward with forward

        File actualFilePath = new File(filePath);
        if (deleteOrArchiveSourceFile.equalsIgnoreCase("archiveSourceFile")) {
            String filePathSpilt[] = filePath.split("/");
            String erwinName = "";
            String archivePath = "";
            String erwinSpiltPath[] = null;

            try {
                if (filePathSpilt.length > 6 && filePath.toLowerCase().contains("erwin/ms sql source")) {
                    erwinName = filePathSpilt[filePathSpilt.length - 6];

                    erwinSpiltPath = filePath.split(erwinName);
                    if (erwinSpiltPath.length >= 2) {
//                        archivePath = filePath.split(erwinName)[0] + erwinName + "/archive" + "/" + filePath.split(erwinName)[1];
                        archivePath = archivePathFromCat + filePath.split(erwinName)[1];
                        archivePath = archivePath.substring(0, archivePath.lastIndexOf("/") + 1);
                    }

                } else {
                    filePathFromCat = filePathFromCat.substring(filePathFromCat.lastIndexOf("/") + 1);
                    erwinSpiltPath = filePath.split(filePathFromCat);
                    if (erwinSpiltPath.length >= 2) {

//                        archivePath = erwinSpiltPath[0] + "archive/MS Sql Source/" + filePathFromCat + erwinSpiltPath[1];
                        archivePath = archivePathFromCat + "MS Sql Source/" + filePathFromCat + erwinSpiltPath[1];
                    }

                    archivePath = archivePath.substring(0, archivePath.lastIndexOf("/") + 1);

                }
            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }
//            archivePath = "C:test/abc.txt";
//            archivePath = "C:erwin/test/TEST";
//            filePath = "C:erwin/test/temp.sql";
//            String output = "C:erwin/test/TEST/temp.sql";
//            output= "C:erwin/test/TEST/TEST/temp.sql";

            File archiveFilePath = null;
            if (!StringUtils.isBlank(archivePath)) {
                archiveFilePath = new File(archivePath);
                if (!archiveFilePath.exists()) {
                    archiveFilePath.mkdirs();
                }
            }

            try {
                if (archiveFilePath != null) {
                    FileUtils.copyFileToDirectory(actualFilePath, archiveFilePath);
                }

            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }

        }

    }

    public dataflow getDataflowFromSql(String sqltext, String fileName, String databaseType) {

        try {

//            TGSqlParser sqlparser = isQueryParsable(sqltext);
            EDbVendor dbVendor = getDBVendorFromStringVendorName(databaseType);
            TGSqlParser sqlparser = isQueryParsable(sqltext, dbVendor);
            if (sqlparser == null) {
                System.out.println("Query is Not able to parse" + "...." + "FileName...." + fileName);
                return null;
            } else {
                System.out.println("Query is Compatible to " + sqlparser.getDbVendor() + "...." + "FileName...." + fileName);
            }
//	    FIX T12 for updating procedure Name and Use Line database
            if (sqlparser.sqlstatements.size() == 1) {
                TCustomSqlStatement customStmt = sqlparser.sqlstatements.get(0);
                switch (customStmt.sqlstatementtype) {
                    case sstmssqlcreateprocedure:
                        TObjectName prName = ((TMssqlCreateProcedure) customStmt).getProcedureName();
                        procedureName = prName != null ? prName.toString() : "";
                        break;
                    case sstcreateview:
                        prName = ((TCreateViewSqlStatement) customStmt).getViewName();
                        procedureName = prName != null ? prName.toString() : "";
                        break;
                    case sstUseDatabase:
                        TObjectName dbName = ((TUseDatabase) customStmt).getDatabaseName();
                        useLine = dbName != null ? dbName.toString() : "";
                        return null;
                    default:
                        System.out.println("");
                }
            }
            fileName = "".equals(procedureName) ? fileName : procedureName;//Fix for RS Name
            fileName = fileName.replace(".", "_").replace("]", "").replace("[", "");
            fileName = fileName.replaceAll("[^a-zA-Z0-9 _-]", "_");
            DataFlowAnalyzer dlineage = new DataFlowAnalyzer(sqltext, fileName, dbVendor, false);
            dlineage.setShowJoin(true);
            dlineage.setIgnoreRecordSet(false);

            StringBuffer errorBuffer = new StringBuffer();
            String result = dlineage.generateDataFlow(errorBuffer);
            intermediateComponents = new HashSet<>();
            dataflow dtflow = XML2Model.loadXML(dataflow.class, result);
            if (dtflow != null) {
                List<table> intComps = dtflow.getResultsets();
                for (table intMediate : intComps) {
                    intermediateComponents.add(intMediate.getName().toUpperCase());
                    intermediateComponents.add("JOIN_" + intMediate.getName().toUpperCase());
                }
            }
            return dtflow;

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static String createMappingFromJson(String mappingJson, String mapName, int subjectId, int projectId,
            MappingManagerUtil maputil, String jsonFilePath, String folderServerName, String folderDatabaseName,
            String schemaNameFromFolder, String fileName, String loadType, String executionDateTime, String logFilePath,
            String filePath, String sqlText, String deleteOrArchiveSourceFile, String filePathFromCat, String archivePath) {
        String status = "";
        Mapping mapping = null;
        try {
            try {
                folderServerName = folderServerName.replaceAll("[^a-zA-Z0-9]", "_");
                folderDatabaseName = folderDatabaseName.replaceAll("[^a-zA-Z0-9]", "_");
                useLine = useLine.replaceAll("[^a-zA-Z0-9]", "_");
            } catch (Exception e) {
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }
            String schemaName = "";
            if (StringUtils.isBlank(schemaNameFromFolder)) {
                schemaName = defaultSchemaName;
            } else {
                schemaName = schemaNameFromFolder;
            }

            folderDatabaseName = folderDatabaseName + DELIMITER + useLine;//FIX T5 Adding Delimiter

            ArrayList<MappingSpecificationRow> SpecsLists = SyncMetadataJFDServerDBSchema.
                    setMetaDataSpec(mappingJson, folderDatabaseName, folderServerName, jsonFilePath, 
                            defaultSystemName, defaultEnviormentName, metadatChacheHM,  
                            allDBMap, schemaName, DELIMITER,intermediateComponents);

            mapping = new Mapping();
            mapping.setMappingName(mapName);
            mapping.setProjectId(projectId);
            mapping.setSubjectId(subjectId);
            mapping.setMappingSpecifications(SpecsLists);
            mapping.setSourceExtractQuery(sqlText);

            ObjectMapper objectMapper = new ObjectMapper();
            mappingJson = objectMapper.writeValueAsString(mapping);

            mappingJson = mappingJson.replace(",\"childNodes\":[]", "");

            int mappingId = -1;

            try {

                mappingId = getMappingId(subjectId, mapName, projectId, maputil);

            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }

            if (mappingId > 0 && loadType.equalsIgnoreCase("full")) {
                String deleteStatus = "";
                if (subjectId > 0) {
                    deleteStatus = maputil.deleteMappingAs(subjectId, "MM_SUBJECT", mapName, "ALL_VERSIONS", 0.0f, "json");
                } else {
                    deleteStatus = maputil.deleteMappingAs(projectId, "MM_PROJECT", mapName, "ALL_VERSIONS", 0.0f, "json");
                }

                deleteStatus = getStatusWithFileAndMapName(deleteStatus, fileName, mapName, loadType, filePath);
                String createStatus = maputil.createMappingAs(mappingJson, "json");
                createStatus = getStatusWithFileAndMapName(createStatus, fileName, mapName, loadType, filePath);

                mappingId = getMappingId(subjectId, mapName, projectId, maputil);

                status = status + "\n" + deleteStatus + "\n" + createStatus;

            } else if (mappingId > 0 && loadType.equalsIgnoreCase("incremental")) {
//                String versionStatusWithLatestMapId = CreateMappingVersion.preCreatingMapVersion(mappingJson, projectId, mapName, maputil, subjectId, kvUtil);

                String versionStatusWithLatestMapId = CreateMappingVersion.creatingMapVersionForIncremental(projectId, mapName, subjectId, SpecsLists, maputil, kvUtil);
                String versionStatus = "";

                if (versionStatusWithLatestMapId.contains(DELIMITER) && versionStatusWithLatestMapId.split(DELIMITER).length >= 2) {//FIX T5 Adding Delimiter
                    versionStatus = versionStatusWithLatestMapId.split(DELIMITER)[0];
                    String mapStringId = versionStatusWithLatestMapId.split(DELIMITER)[1];
                    mapStringId = mapStringId.replace("\"", "");
                    mappingId = Integer.parseInt(mapStringId);

                }
                status = status + "\n" + getStatusWithFileAndMapName(versionStatus, fileName, mapName, loadType, filePath) + "\n";
            } else {
                String newStatus = maputil.createMappingAs(mappingJson, "JSON");
                status = status + "\n" + getStatusWithFileAndMapName(newStatus, fileName, mapName, loadType, filePath) + "\n";

                try {

                    mappingId = getMappingId(subjectId, mapName, projectId, maputil);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (mappingId > 0) {

                if (deleteOrArchiveSourceFile.equalsIgnoreCase("archiveSourceFile") && !(filePathFromCat.contains("vUpload"))) {

                    archiveSourceFiles(filePath, deleteOrArchiveSourceFile, filePathFromCat, archivePath);
                }
                if (deleteOrArchiveSourceFile.equalsIgnoreCase("archiveSourceFile") || deleteOrArchiveSourceFile.equalsIgnoreCase("deleteSourceFile")) {
                    File actualFilePathFile = new File(filePath);
                    actualFilePathFile.delete();
                }

            }

            try {
//                status = status + addKeyValues(mappingId, keyValuesDeailsMap, kvUtil);
                addKeyValues(mappingId, keyValuesDeailsMap, kvUtil);
            } catch (Exception e) {
                e.printStackTrace();
                exceptionBuilder.append("Exception Details: " + e + "\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
            exceptionBuilder.append("Exception Details: " + e + "\n");
        }
        return status;
    }

    // Added By Dinesh On June_22_2020
    public String getMappingObjectToJsonForSSIS(String sqlFileContent, String defaultSystemName, String defaultEnvironmentName, int projectId, String databaseType, String mapName, int subjectId) {
        Mapping mapping = null;
        String mappingJson = "";

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // getAllEnvironments(systemManagerUtil);

            String systemAndEnvironmentDeatils[] = {defaultSystemName, defaultEnvironmentName, defaultSystemName, defaultEnvironmentName};
            String sqltext = "";
            sqltext = ErwinSQLWrapper.removeUnparsedDataFromQuery(sqlFileContent);

            try {
                if (sqltext.trim().startsWith("USE")) {
                    useLine = sqltext.split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
                }
            } catch (Exception e) {
//                e.printStackTrace();
            }

            dataflow dtflow = getDataflowFromSql(sqltext, mapName, databaseType);
            if (dtflow == null) {
                return "";
            }
            RelationAnalyzer relationAnalyzer = new RelationAnalyzer();

            ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, systemAndEnvironmentDeatils);
            keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();
            if (mapSpecRows == null || mapSpecRows.isEmpty()) {
                return "";
            }

            mapping = new Mapping();
            mapping.setMappingName(mapName);
            mapping.setProjectId(projectId);
            mapping.setSubjectId(subjectId);
            mapping.setMappingSpecifications(mapSpecRows);
            mapping.setSourceExtractQuery(sqltext);

            mappingJson = objectMapper.writeValueAsString(mapping);
            if (mappingJson != null || !mappingJson.isEmpty()) {
                return mappingJson;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return "";

    }

    public String getMappingObjectToJsonForReport(String sqlFileContent, String defaultSystemName, String defaultEnvironmentName, int projectId, String databaseType, String mapName, int subjectId) {
        Mapping mapping = null;
        String mappingJson = "";

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // getAllEnvironments(systemManagerUtil);

            String systemAndEnvironmentDeatils[] = {defaultSystemName, defaultEnvironmentName, defaultSystemName, defaultEnvironmentName};
            String sqltext = "";
            sqltext = ErwinSQLWrapper.removeUnparsedDataFromQuery(sqlFileContent);

            try {
                if (sqltext.trim().startsWith("USE")) {
                    useLine = sqltext.split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            dataflow dtflow = getDataflowFromSql(sqltext, mapName, databaseType);
            if (dtflow == null) {
                return "";
            }
            RelationAnalyzer relationAnalyzer = new RelationAnalyzer();

            ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, systemAndEnvironmentDeatils);
            keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();
            if (mapSpecRows == null || mapSpecRows.isEmpty()) {
                return "";
            }

            mapping = new Mapping();
            mapping.setMappingName(mapName);
            mapping.setProjectId(projectId);
            mapping.setSubjectId(subjectId);
            mapping.setMappingSpecifications(mapSpecRows);
            mapping.setSourceExtractQuery(sqltext);

            mappingJson = objectMapper.writeValueAsString(mapping);
            if (mappingJson != null || !mappingJson.isEmpty()) {
                return mappingJson;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return "";

    }

    public static int createSubject(String subjectName, int projectId, MappingManagerUtil maputil) {
        try {
            Project project = maputil.getProject(projectId);
            String projectName = project.getProjectName();
            int subjectId = maputil.getSubjectId(projectName, subjectName);

            if (subjectId > 0) {
                return subjectId;
            }

            Subject subjectDetails = new Subject();
            subjectDetails.setSubjectName(subjectName);
            subjectDetails.setSubjectDescription("Oracle and sql Details");
            subjectDetails.setProjectId(projectId);
            subjectDetails.setConsiderUserDefinedFlag("Y");
            subjectDetails.setParentSubjectId(-1);

            RequestStatus retRS = maputil.createSubject(subjectDetails);
            if (retRS.isRequestSuccess()) {

                subjectId = maputil.getSubjectId(projectName, subjectName);
                return subjectId;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private TGSqlParser isQueryParsable(String sqltext, EDbVendor vendor) {
        TGSqlParser sqlparser = new TGSqlParser(vendor);
        sqlparser.sqltext = sqltext;
        int parsedResult = sqlparser.parse();
        
        
        if (parsedResult ==0) {
            return sqlparser;
        } else {
            return null;
        }
    }

    public static String addKeyValues(int mappingId, LinkedHashMap<String, HashSet<String>> keyValuesHashMap, KeyValueUtil keyValueUtil) {
        String status = "";
        try {

            if (mappingId > 0 && keyValuesHashMap != null && !keyValuesHashMap.isEmpty()) {
                Set<String> conditionTypeSet = keyValuesHashMap.keySet();
                Iterator<String> ctItr = conditionTypeSet.iterator();

                List<KeyValue> keyValuesList = new ArrayList();
                while (ctItr.hasNext()) {
                    String conditionType = ctItr.next();
                    List<KeyValue> tkv = addSpecificKeyValues(keyValuesHashMap.get(conditionType), getKeyType(conditionType));

                    if (tkv != null && !tkv.isEmpty()) {
                        keyValuesList.addAll(tkv);
                    }

                }
                if (keyValuesList != null && !keyValuesList.isEmpty()) {

                    RequestStatus rs = keyValueUtil.addKeyValues(keyValuesList, Node.NodeType.MM_MAPPING, mappingId);
                    status = status + rs.getStatusMessage();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static String getKeyType(String conditionType) {
        if ("JOIN_CONDITION".equals(conditionType)) {
            return "Join";
        } else if ("WHERE_CONDITION".equals(conditionType)) {
            return "Where condition";
        } else if ("GROUPBY_CONDITION".equals(conditionType)) {
            return "Group by";
        } else if ("ORDERBY_CONDITION".equals(conditionType)) {
            return "Order by";
        } else {
            return "CONDITION";
        }

    }

    public static List<KeyValue> addSpecificKeyValues(Set<String> conditionsSet, String pKeyType) {

        if (conditionsSet != null && !conditionsSet.isEmpty()) {

            List<KeyValue> keyValuesList = new ArrayList();
            Iterator<String> cItr = conditionsSet.iterator();
            int icr = 0;
            while (cItr.hasNext()) {

                String condition = cItr.next();
                String keyType = pKeyType;
                //adding join type
                if ("Join".equalsIgnoreCase(keyType)) {
                    String[] tempCndArr = condition.split(DELIMITER);
                    condition = tempCndArr[0];
                    if (!"Join".equalsIgnoreCase((tempCndArr[1]).trim())) {
                        keyType = tempCndArr[1] + " " + keyType;
                    }
                }

                if (condition != null && condition.length() > 0) {

                    condition = removeSpaces(condition);
                    String key = keyType + " " + (++icr);

                    //if value length is more than 500, we are to split and adding
                    if (condition.length() > 500) {
                        int j = 0;

                        if (StringUtils.isBlank(key)) {
                            continue;
                        }
                        String value = "";
                        while (condition.length() > 500) {
                            value = condition.substring(0, 500);
                            if (value == null) {
                                continue;
                            }

                            keyValuesList.add(buildKeyValue(key + " part_" + (++j), value));
                            condition = condition.substring(500);
                        }
                        value = condition;
                        if (value == null) {
                            continue;
                        }
                        keyValuesList.add(buildKeyValue(key + " part_" + (++j), value));
                    } else {
                        //    String value = condition;
                        if (condition == null) {
                            continue;
                        }
                        keyValuesList.add(buildKeyValue(key, condition));
                    }
                }

            }
            return keyValuesList;
        }
        return null;
    }

    public static String removeSpaces(String str) {
        return str.trim().replace("\t", " ").replaceAll("( )+", " ");
    }

    public static KeyValue buildKeyValue(String key, String value) {

        KeyValue kv = new KeyValue(key, value);
        kv.setPublished(true);
        kv.setVisibility(1);
        return kv;
    }

    public static int createChildSubject(String subjectName, int projectId, int parentSubId, MappingManagerUtil mappingManagerUtil) {
        StringBuilder sb = new StringBuilder();
        Subject subjectDetails = new Subject();
        int subjectId = 0;

        try {
            subjectId = mappingManagerUtil.getSubjectId(parentSubId, Node.NodeType.MM_SUBJECT, subjectName);
            if (subjectId > 0) {
                return subjectId;
            }
        } catch (Exception e) {

        }

        //  Subject subjectDetails = new Subject();
        subjectDetails.setSubjectName(subjectName);
        subjectDetails.setSubjectDescription("Oracle and sql Details_Child");
        subjectDetails.setProjectId(projectId);
        subjectDetails.setConsiderUserDefinedFlag("Y");
        subjectDetails.setParentSubjectId(parentSubId);

        try {
            RequestStatus retRS = mappingManagerUtil.createSubject(subjectDetails);
            sb.append(subjectName + " " + retRS.getStatusMessage() + "\n\n");
//            if (retRS.isRequestSuccess()) {

            subjectId = mappingManagerUtil.getSubjectId(parentSubId, Node.NodeType.MM_SUBJECT, subjectName);
            //     subjectId = mappingManagerUtil.getSubjectId(projectName, subjectName);
            return subjectId;
//            }
        } catch (Exception e) {

        }
        return subjectId;
    }

    public static String getStatusWithFileAndMapName(String status, String fileName, String mapName, String loadType, String filePath) {
        try {

            if (status.contains("}")) {
                String statusSpilt[] = status.split("}");
                status = statusSpilt[0].trim() + "," + "\n";
                status = status + " \"FileName \" : " + "\"" + fileName + "\"" + "," + "\n";
                status = status + " \"FilePath \" : " + "\"" + filePath + "\"" + "," + "\n";
                status = status + " \"MapName \": " + "\"" + mapName + "\"" + "\n" + "}" + "\n" + "}";
            } else if (loadType.equalsIgnoreCase("incremental")) {
                status = " \"statusMessage \" :" + status + "," + "\n";
                status = status + " \"FileName \" : " + "\"" + fileName + "\"" + "," + "\n";
                status = status + " \"FilePath \" : " + "\"" + filePath + "\"" + "," + "\n";
                status = "{\n" + status + " \"MapName \": " + "\"" + mapName + "\"" + " \n}";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    public static int getMappingId(int subjectId, String mapName, int projectId, MappingManagerUtil mappingManagerUtil) {
        int mappingId = 0;
        try {
            if (subjectId > 0) {
                mappingId = mappingManagerUtil.getMappingId(subjectId, mapName, Node.NodeType.MM_SUBJECT);
            } else {
                mappingId = mappingManagerUtil.getMappingId(projectId, mapName, Node.NodeType.MM_PROJECT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mappingId;
    }

    public static void createFile(String filePath, String executionDateTime, String content, String fileType, String fileName, String fileExtension) {
        File file = null;
        try {
            String outputMetadataFilePath = "";
            if (fileType.equalsIgnoreCase("logData")) {
                file = new File(filePath);
                if (!file.exists()) {
                    file.mkdirs();
                }
                outputMetadataFilePath = filePath + "/" + fileName + executionDateTime + "." + fileExtension;
            } else if (fileType.equalsIgnoreCase("archiveFile")) {
                outputMetadataFilePath = filePath + fileName + "." + fileExtension;
            }
            file = new File(outputMetadataFilePath);
            FileUtils.write(file, content, "UTF-8");//FIX T11 for Writing Content into file
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static EDbVendor getDBVendorFromStringVendorName(String dbName) {
        EDbVendor dbVendor = EDbVendor.dbvmssql;
        if ("oracle".equals(dbName)) {
            dbVendor = EDbVendor.dbvoracle;
        } else if ("mssql".equals(dbName)) {
            dbVendor = EDbVendor.dbvmssql;
        } else if ("postgresql".equals(dbName)) {
            dbVendor = EDbVendor.dbvpostgresql;
        } else if ("redshift".equals(dbName)) {
            dbVendor = EDbVendor.dbvredshift;
        } else if ("odbc".equals(dbName)) {
            dbVendor = EDbVendor.dbvodbc;
        } else if ("mysql".equals(dbName)) {
            dbVendor = EDbVendor.dbvmysql;
        } else if ("netezza".equals(dbName)) {
            dbVendor = EDbVendor.dbvnetezza;
        } else if ("firebird".equals(dbName)) {
            dbVendor = EDbVendor.dbvfirebird;
        } else if ("access".equals(dbName)) {
            dbVendor = EDbVendor.dbvaccess;
        } else if ("ansi".equals(dbName)) {
            dbVendor = EDbVendor.dbvansi;
        } else if ("generic".equals(dbName)) {
            dbVendor = EDbVendor.dbvgeneric;
        } else if ("greenplum".equals(dbName)) {
            dbVendor = EDbVendor.dbvgreenplum;
        } else if ("hive".equals(dbName)) {
            dbVendor = EDbVendor.dbvhive;
        } else if ("sybase".equals(dbName)) {
            dbVendor = EDbVendor.dbvsybase;
        } else if ("hana".equals(dbName)) {
            dbVendor = EDbVendor.dbvhana;
        } else if ("impala".equals(dbName)) {
            dbVendor = EDbVendor.dbvimpala;
        } else if ("dax".equals(dbName)) {
            dbVendor = EDbVendor.dbvdax;
        } else if ("vertica".equals(dbName)) {
            dbVendor = EDbVendor.dbvvertica;
        } else if ("couchbase".equals(dbName)) {
            dbVendor = EDbVendor.dbvcouchbase;
        } else if ("snowflake".equals(dbName)) {
            dbVendor = EDbVendor.dbvsnowflake;
        } else if ("openedge".equals(dbName)) {
            dbVendor = EDbVendor.dbvopenedge;
        } else if ("informix".equals(dbName)) {
            dbVendor = EDbVendor.dbvinformix;
        } else if ("teradata".equals(dbName)) {
            dbVendor = EDbVendor.dbvteradata;
        } else if ("mdx".equals(dbName)) {
            dbVendor = EDbVendor.dbvmdx;
        } else if ("db2".equals(dbName)) {
            dbVendor = EDbVendor.dbvdb2;
        }
        return dbVendor;
    }

    public static String writeExeceptionDeatilsIntoFile(boolean flag, int storeProcSize, int exceptionCount, StringBuilder outerExceptionBuilder, StringBuilder exceptionBuilder, String logFilePath, String executionDateTime) {
        String individualStatus = "";
        try {

            if (flag || exceptionCount == storeProcSize) {
                if (exceptionCount == storeProcSize && exceptionBuilder.length() > 0) {
                    individualStatus = exceptionBuilder.toString();
                } else {
                    individualStatus = outerExceptionBuilder.toString();
                }

                individualStatus = individualStatus + "\n";
                String logFileName = "SQL Parser log_";
                String fileExtension = "log";
                createFile(logFilePath, executionDateTime, individualStatus, "logData", logFileName, fileExtension);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return individualStatus;
    }

}
