/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.sqlparser;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.dataflow.model.xml.dataflow;
import com.erwin.sqlparser.wrapper.parser.ErwinSQLWrapper;
import com.erwin.util.XML2Model;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.pp.para.GFmtOpt;
import gudusoft.gsqlparser.pp.para.GFmtOptFactory;
import gudusoft.gsqlparser.pp.stmtformatter.FormatterFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author Trinesh
 */
public class MappingCreator {

    private LinkedHashMap<String, HashSet<String>> keyValuesDeailsMap;
    public static EDbVendor dbVendor = EDbVendor.dbvmssql;

    public LinkedHashMap<String, HashSet<String>> getKeyValuesDeailsMap() {
        return this.keyValuesDeailsMap;
    }

    public static void main(String[] args) {

//        String fileDirectoryPath = "D:\\Projects\\SNHU\\ParsedFiles";
//        String fileDirectoryPath = "D:\\Projects\\SNHU\\Issues";
//        String fileDirectoryPath = "D:\\Projects\\SNHU\\Test";
        String fileDirectoryPath = "D:\\Projects\\SNHU\\New\\SQL\\MS SQL Source\\SQL_2D_COCE_BI\\AARDA\\dbo\\";
        fileDirectoryPath += "FactStudentLead_Load_Update.sql";
        String[] sysEnvDetails = {"SrcSystem", "SrcEnv", "TgtSystem", "TgtEnv"};
        String vendor = "mssql";
        MappingCreator creator = new MappingCreator();
        dbVendor = creator.getDBVendor(vendor);
        File dir = new File(fileDirectoryPath);
        creator.search(".*\\.sql", dir, sysEnvDetails);

    }

    public dataflow getDataflowFromSql(String sqltext, String fileName, String[] sysenvDetails) {

        try {

//            TGSqlParser sqlparser = isQueryParsable(sqltext);
            TGSqlParser sqlparser = isQueryParsable(sqltext, dbVendor);
            if (sqlparser == null) {
                System.out.println("Query is Not able to parse");
                return null;
            } else {
                System.out.println("Query is Compatible to " + sqlparser.getDbVendor());
            }
            DataFlowAnalyzer dlineage = new DataFlowAnalyzer(sqltext, fileName, dbVendor, false);
            dlineage.setShowJoin(true);
            dlineage.setIgnoreRecordSet(false);

            StringBuffer errorBuffer = new StringBuffer();
            String result = dlineage.generateDataFlow(errorBuffer);

            File xmlFile = new File("C:\\Users\\TrineshVanguri\\Desktop\\Version8_3.xml");
            FileUtils.writeStringToFile(xmlFile, result, "UTF-8");
            return XML2Model.loadXML(dataflow.class, result);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ArrayList<MappingSpecificationRow> getMappingSpecList(String inputFilePath, String[] sysenvDetails) {
        try {

            File inputFile = new File(inputFilePath);
            String fileName = getFileName(inputFile.getName());
            Set<String> queries = ErwinSQLWrapper.getAllStatementsForSqlFile(inputFilePath, dbVendor);
            for (String sqltext : queries) {
                sqltext = ErwinSQLWrapper.removeUnparsedDataFromQuery(sqltext);

                dataflow dtflow = getDataflowFromSql(sqltext, fileName, sysenvDetails);
                if (dtflow == null) {
                    return new ArrayList<>();
                }
                RelationAnalyzer relationAnalyzer = new RelationAnalyzer();

                ArrayList<MappingSpecificationRow> mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
                this.keyValuesDeailsMap = relationAnalyzer.getKeyValuesMap();
                System.out.println("MapSpecRow Size ==> "+mapSpecRows.size());
                int i = 1;
                for (MappingSpecificationRow mspecRow : mapSpecRows) {
                    i = i + 1;
                    System.out.println(i + ")" + mspecRow.getSourceTableName() + "====>" + mspecRow.getSourceColumnName() + "====>"
                            + mspecRow.getBusinessRule() + "===>" + mspecRow.getTargetTableName() + "====>" + mspecRow.getTargetColumnName());
                }

//                System.out.println("keyValuesMap ===> " + this.keyValuesDeailsMap);
            }
            return new ArrayList<>();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public Mapping getMappingObject(String inputFilePath, String[] sysenvDetails, int projectId, int subjectId) {
        Mapping mapping = null;
        try {

            File inputFile = new File(inputFilePath);
            String fileName = getFileName(inputFilePath);
            String sqltext = FileUtils.readFileToString(inputFile, "UTF-8");

            dataflow dtflow = getDataflowFromSql(sqltext, fileName, sysenvDetails);
            if (dtflow == null) {
                return null;
            }
            System.out.println("keyValuesMap ===> " + this.keyValuesDeailsMap);

            mapping = new Mapping();
            mapping.setMappingName(fileName);
            mapping.setProjectId(projectId);
            mapping.setSubjectId(subjectId);
            mapping.setMappingSpecifications(null);
            mapping.setSourceExtractQuery(sqltext);

        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return mapping;

    }

    private TGSqlParser isQueryParsable(String sqltext) {
        int parsedResult = -1;
        TGSqlParser sqlparser = null;
        List<EDbVendor> dbvenderlist = Arrays.asList(EDbVendor.values());
        for (EDbVendor vendor : dbvenderlist) {
            sqlparser = new TGSqlParser(vendor);
            sqlparser.sqltext = sqltext;
            parsedResult = sqlparser.parse();
            if (parsedResult == 0) {
                break;
            } else {
                sqlparser = null;
            }
        }
        return sqlparser;
    }

    private TGSqlParser isQueryParsable(String sqltext, EDbVendor vendor) {
        TGSqlParser sqlparser = new TGSqlParser(vendor);
        sqlparser.sqltext = sqltext;
        int parsedResult = sqlparser.parse();
        if (parsedResult == 0) {
            return sqlparser;
        } else {
            return null;
        }
    }

    public void search(String pattern, File folder, String[] sysEnvDetails) {
        if (folder.isFile()) {
            if (folder.getName().matches(pattern)) {
                System.out.println("------------- FileName is--->" + folder.getAbsolutePath() + "<----------------");
                MappingCreator mappingCreator = new MappingCreator();
                mappingCreator.getMappingSpecList(folder.getAbsolutePath(), sysEnvDetails);
            }
        } else {
            for (File f : folder.listFiles()) {
                if (f.isDirectory()) {
                    search(pattern, f, sysEnvDetails);
                }
                if (f.isFile()) {
                    if (f.getName().matches(pattern)) {
                        System.out.println("------------- FileName is--->" + f.getAbsolutePath() + "<----------------");
                        MappingCreator mappingCreator = new MappingCreator();
                        mappingCreator.getMappingSpecList(f.getAbsolutePath(), sysEnvDetails);
                    }
                }

            }
        }
    }

    private String getFileName(String name) {
        if (name == null || name.lastIndexOf(".") < 0) {
            return "";
        }
        return name.substring(0, name.lastIndexOf("."));
    }

    public String formatSQLQuery(String sqlContent) {
        try {
            //format query
            TGSqlParser gSqlParser = new TGSqlParser(dbVendor);
            gSqlParser.setSqltext(sqlContent);
            GFmtOpt option = GFmtOptFactory.newInstance();
            sqlContent = FormatterFactory.pp(gSqlParser, option);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sqlContent;
    }

    public static String removeComments(String sql) {
        String regexPattern = "(--.*)|(((/\\*\\*)+?[\\w\\W]+?(\\*\\*/)+))";
        sql = sql.replaceAll(regexPattern, "");
        /*Removing extra *\/ with empty */
        regexPattern = "(--.*)|(((/\\*)+?[\\w\\W]+?(\\*/)+))";
        sql = sql.replaceAll(regexPattern, "");
        return sql;

    }

    public EDbVendor getDBVendor(String dbName) {
        dbVendor = EDbVendor.dbvmssql;
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

}
