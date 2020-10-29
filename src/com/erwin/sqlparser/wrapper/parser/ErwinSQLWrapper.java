/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.sqlparser.wrapper.parser;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.ESqlStatementType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author NarsimuluKondapaka
 */
/**
 *
 * This class methods are to parse procedure/any query to gudusoft able to parse
 * query.
 *
 */
public class ErwinSQLWrapper {

    public static String useLine = "";
    public static StringBuilder exceptionBuilder;
    public static String fileName;
    public static String filePath = "";//"D:/Projects/SNHU/New/ModifiedFiles/RPT";

    public static Set<String> getAllStatementsForSqlFile(String inputfile, EDbVendor dbVender) throws Exception {
        String sqlContent = FileUtils.readFileToString(new File(inputfile), "UTF-8");
        fileName = FilenameUtils.getName(inputfile);
        TGSqlParser sqlparser = new TGSqlParser(dbVender);
        sqlparser.sqlfilename = inputfile;
        int ret = sqlparser.getrawsqlstatements();
        if (ret == 0) {
            Set<String> queries = checksqlContent(sqlparser);
            return queries;
        }
        sqlContent = sqlContent.replaceAll("[^\\p{ASCII}]", " ");//For Replacing Non-Asc characters
        sqlContent = sqlContent.replaceAll("FOR FETCH ONLY", " ");//becuase it is dividing as 2 statements we are replacing it here
        Set<String> queries = getAllStatements(sqlContent, dbVender);
        return queries;
    }

    public static Set<String> getAllStatements(String sqlContent, EDbVendor dbVender) throws Exception {
        long t = System.currentTimeMillis();
        Set<String> storeprocfile = null;
        TGSqlParser sqlparser = new TGSqlParser(dbVender);
        sqlparser.sqltext = sqlContent;
        int ret = sqlparser.getrawsqlstatements();
        if (ret == 0) {
            storeprocfile = checksqlContent(sqlparser);
        } else {
            throw new Exception(sqlparser.getErrormessage());
        }
        return storeprocfile;
    }

    public static Set<String> checksqlContent(TGSqlParser sqlparser) {
        Set<String> storeprocfile = new LinkedHashSet();
        String sqlContent = "";
        for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
            TCustomSqlStatement customStmt = sqlparser.sqlstatements.get(i);
            if (customStmt.sqlstatementtype == ESqlStatementType.sstmssqlgo
                    || customStmt.sqlstatementtype == ESqlStatementType.sstmssqlset) {//Need to check
                continue;
            }
            String query = removeUnparsedDataFromQuery(customStmt.toString());
            storeprocfile.add(query);
            sqlContent += "\n" + query;
        }
//        createModifiedFiles(filePath, sqlContent); //Need to comment while delivering
        return storeprocfile;
    }

    public static String removeUnparsedDataFromQuery(String sqlText) {
        String parsedData = "";
        boolean flag = true;
        try {

            String spiltSqlFileData[] = sqlText.split("\n");
             
            for (String data : spiltSqlFileData) {
               
                if (StringUtils.isBlank(data)) {
                    continue;
                }
                // the 1st space in the downline is some special space character from the query that's why the query is not parsing so we replaced
                // the special space character with normal space character in the downline
                data = data.replaceAll(" ", " ");
                data = data.toUpperCase().replace("FROM ISNULL", "+ ISNULL");

                try {
                    if (data.startsWith("USE")) {
                        useLine = data.split(" ")[1].trim().replace("[", "").replace("]", "").toUpperCase();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptionBuilder.append("Exception Details: " + e + "\n");
                }
                if (data.toUpperCase().contains("OPTION") && data.toUpperCase().contains("USE")) {
                    try {
                        data = data.toUpperCase().replace("OPTION", ";--OPTION");
                        parsedData = parsedData + data + "\n";
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (!data.trim().toUpperCase().startsWith("CREATE") && data.toUpperCase().contains("INDEX") && data.toUpperCase().contains("CLUSTERED")) {
                    continue;
                } else if (data.trim().toUpperCase().startsWith("ALTER TABLE")) {
                    flag = false;
                  
                    if (data.trim().endsWith(";")) {// FIX 11 Written for Alter statement ending only
                        flag = true;
                    }
                    continue;
                }
                if(!flag&&data.trim().toUpperCase().startsWith("ADD")){// For Removing the Add Statement after Alter.
                continue;
                }
                
                 
                else if (data.trim().toUpperCase().contains("PERCENTILE_DISC(.5)")) {
                    continue;
                } else {

                    try {
                        if (data.contains("CASE") && data.contains("?") || data.contains("Â€‹") || data.contains("â€‹")) {//Â€‹
                            data = data.replaceAll("\\?", "").replaceAll("Â€‹", "").replaceAll(" CASE", "CASE").replaceAll("â€‹", "");
                        }

                        data = data.toUpperCase().replaceAll("AT TIME ZONE 'EASTERN STANDARD TIME'", "");
                        data = data.toUpperCase().replaceAll("FOR SYSTEM_TIME ALL", "");
                        data = data.toUpperCase().replaceAll("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", "");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (data.toUpperCase().contains("SELECT") || data.toUpperCase().contains("UPDATE")
                            || data.toUpperCase().contains("DELETE") || data.toUpperCase().contains("INSERT")
                            || data.toUpperCase().contains("SET") || data.toUpperCase().contains("DECLARE")
                            || data.toUpperCase().contains("PRINT") || data.toUpperCase().contains("TRUNCATE") || data.toUpperCase().contains("IF EXISTS")
                            || data.toUpperCase().contains("END")) {//FIX 12 END is written as some files having END after Alter
                        flag = true;
                    }
                    if (flag) {
                        parsedData = parsedData + data + "\n";
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            exceptionBuilder.append("Exception Details: " + e + "\n");
        }
        return parsedData;
    }

    public static void createModifiedFiles(String filePath, String content) {
        try {
            FileUtils.forceMkdir(new File(filePath));
            FileUtils.writeStringToFile(new File(filePath + "/" + fileName), content, "UTF-8");
        } catch (IOException ex) {
            Logger.getLogger(ErwinSQLWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
