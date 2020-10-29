package com.erwin.sqlparser.util;

import com.erwin.sqlparser.wrapper.parser.ErwinSQLWrapper;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author TrineshVanguri
 */
public class QueriesFileParser {

    public static EDbVendor dbVendor = null;
    public static StringBuilder sb = new StringBuilder();
    public static int fileCount = 1;

    public static void main(String[] args) {
//        String fileDirectoryPath = "D:\\Projects\\SNHU\\New\\SNHU_10";
        String fileDirectoryPath = "D:\\Projects\\SNHU\\New\\SNHU_40";
//        String fileDirectoryPath = "D:\\Projects\\SNHU\\New\\SNHU_1100";
        String vendor = "mssql";
        File dir = new File(fileDirectoryPath);
        System.out.println(parseFiles(dir, vendor));
    }

    {
        sb = new StringBuilder();
        fileCount = 1;
    }

    public static String parseFiles(File folder, String vendor) {
        if (dbVendor == null) {
            dbVendor = getDBVendor(vendor);
        }
        for (File f : folder.listFiles()) {
            if (f.isDirectory()) {
                parseFiles(f, vendor);
            }
            if (f.isFile()) {
                sb.append((fileCount++)+"------------- FileName is--->" + f.getAbsolutePath() + "<----------------\n");
                TGSqlParser parser = isQueryParsable(f.getAbsolutePath(), dbVendor);
                sb.append("----------------------------------------------------------------\n");
            }
        }
        return sb.toString();
    }

    private static TGSqlParser isQueryParsable(String filePath, EDbVendor vendor) {

        TGSqlParser sqlparser = new TGSqlParser(vendor);
        sqlparser.sqlfilename = filePath;
        int parsedResult = sqlparser.parse();
        if (parsedResult == 0) {
            sb.append("Query  is Parseable\n");
            return sqlparser;
        } else {
            try {
                Set<String> queries = ErwinSQLWrapper.getAllStatementsForSqlFile(filePath, vendor);
                int i = 0;
                for (String query : queries) {
                    query = ErwinSQLWrapper.removeUnparsedDataFromQuery(query);
                    sqlparser.sqltext = query;
                    parsedResult = sqlparser.parse();
                    if (parsedResult == 0) {
                        sb.append("Query No " + (i++) + " is Parseable\n");
                    } else {
                        sb.append("Query No " + (i++) + " is Not Parseable\n");
                    }
                }
            } catch (Exception e) {
                sb.append("Error Message ==> "+sqlparser.getErrormessage()+"\n");
            } finally {
                if (parsedResult == 0) {
                    return sqlparser;
                } else {
                    return null;
                }
            }
        }

    }

    public static EDbVendor getDBVendor(String dbName) {
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
