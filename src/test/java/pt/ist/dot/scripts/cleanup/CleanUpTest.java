package pt.ist.dot.scripts.cleanup;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import pt.ist.fenixframework.DomainModelParser;
import pt.ist.fenixframework.core.DmlFile;
import pt.ist.fenixframework.core.Project;
import pt.ist.fenixframework.dml.DomainClass;
import pt.ist.fenixframework.dml.DomainModel;

import com.google.common.collect.Maps;

public class CleanUpTest {

    private static DomainModel domainModel;

    private static Set<String> classNames;

    private static String dbAlias;
    private static String username;
    private static String password;
    private static String dbName;

    private static Connection connection;

    @BeforeClass
    public static void setUpDomainModel() throws Exception {
        Project project = Project.fromName("dot-cleaner");
        List<URL> dmls = new ArrayList<URL>();
        for (DmlFile dmlFile : project.getFullDmlSortedList()) {
            dmls.add(dmlFile.getUrl());
        }
        domainModel = DomainModelParser.getDomainModel(dmls);
        Assert.assertNotNull(domainModel);

        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("fenix-framework.properties"));

        dbAlias = properties.getProperty("dbAlias");
        username = properties.getProperty("dbUsername");
        password = properties.getProperty("dbPassword");
        dbName = properties.getProperty("dbName");

        Assert.assertNotNull(dbAlias);
        Assert.assertNotNull(username);
        Assert.assertNotNull(password);
        Assert.assertNotNull(dbName);

        connection = DriverManager.getConnection("jdbc:mysql:" + dbAlias, username, password);
        connection.setAutoCommit(false);

        Assert.assertNotNull(connection);

        classNames = new HashSet<String>();

        for (DomainClass domainClass : domainModel.getDomainClasses()) {
            classNames.add(domainClass.getFullName());
        }
    }

    @AfterClass
    public static void shutdown() throws SQLException {
        connection.close();
    }

    @Test
    public void doCleanup() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs =
                statement.executeQuery("select TABLE_NAME from information_schema.columns where table_schema = '" + dbName
                        + "' and COLUMN_NAME like '%OJB_CONCRETE_CLASS%'");

        Set<String> tables = new HashSet<String>();

        while (rs.next()) {
            tables.add(rs.getString(1));
        }

        rs.close();

        rs = statement.executeQuery("SELECT * FROM FF$DOMAIN_CLASS_INFO");

        Map<String, Integer> classes = Maps.newHashMap();

        while (rs.next()) {
            String className = rs.getString("DOMAIN_CLASS_NAME");
            if (className.startsWith("net.sourceforge")) {
                continue;
            }
            if (!classNames.contains(className)) {
                classes.put(className, rs.getInt("DOMAIN_CLASS_ID"));
            }
        }

        StringBuilder inStatement = new StringBuilder("(");
        for (Entry<String, Integer> entry : classes.entrySet()) {
            if (inStatement.length() > 1) {
                inStatement.append(",");
            }
            inStatement.append(entry.getValue());
        }
        inStatement.append(")");

        for (String table : tables) {
            if (table.equals("GENERIC_LOG") || table.equals("SIGNATURE_INTENTION")) {
                continue;
            }
            ResultSet result = statement.executeQuery("SELECT OID FROM " + table + " WHERE OID >> 32 in" + inStatement);
            while (result.next()) {
                System.out.println("DELETE FROM " + table + " WHERE OID = " + result.getString(1) + ";");
            }
            result.close();
        }

        rs.close();
        statement.close();

    }
}
