package web;

/*
 * IBM Confidential
 *
 * OCO Source Materials
 *
 * WLP Copyright IBM Corp. 2014
 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.naming.InitialContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import javax.transaction.Status;
import javax.transaction.UserTransaction;

/**
 * Servlet implementation class Test
 * This class connects to two databases and does a two phase commit transaction.
 */
public class XAFlowServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor.
     */
    public XAFlowServlet() {
        // TODO Auto-generated constructor stub
        System.out.println("XAFLOWSERVLET: Default ctor");
    }

    public void checkRecXAFlow001(HttpServletRequest request,
                                  HttpServletResponse response) throws Exception {
        int serviceID = 0;
        int serviceID2 = 0;
        try
        {
            // Do the lookups on the DS
            final InitialContext ctx = new InitialContext();
            System.out.println("XAFLOWSERVLET: Context is: " + ctx.toString());
            DataSource firstDS = (DataSource) ctx.lookup("java:comp/env/jdbc/myDataSource");
            System.out.println("XAFLOWSERVLET: Have looked up DS: " + firstDS);

            DataSource secondDS = (DataSource) ctx.lookup("java:comp/env/jdbc/secondDataSource");
            System.out.println("XAFLOWSERVLET: Have looked up DS: " + secondDS);

            // Get connection to database via first datasource
            Connection con1 = firstDS.getConnection();
            System.out.println("XAFLOWSERVLET: Got connection: " + con1);
            DatabaseMetaData mdata = con1.getMetaData();
            System.out.println("XAFLOWSERVLET: Got metadata: " + mdata);
            String dbName = mdata.getDatabaseProductName();
            String dbVersion = mdata.getDatabaseProductVersion();
            System.out.println("XAFLOWSERVLET: You are now connected to " + dbName + ", version " + dbVersion);

            // Get connection to database via second datasource
            Connection con2 = secondDS.getConnection();
            System.out.println("XAFLOWSERVLET: Got connection: " + con2);
            DatabaseMetaData mdata2 = con2.getMetaData();
            System.out.println("XAFLOWSERVLET: Got metadata: " + mdata2);
            String dbName2 = mdata2.getDatabaseProductName();
            String dbVersion2 = mdata2.getDatabaseProductVersion();
            System.out.println("XAFLOWSERVLET: You are now connected to " + dbName2 + ", version " + dbVersion2);

            // Execute a Query against first connection, to check the value of the service_id
            System.out.println("create a statement");
            Statement stmtBasic = con1.createStatement();
            ResultSet rsBasic = null;

            System.out.println("Execute a query to check the value of the service_id in the first database");

            rsBasic = stmtBasic.executeQuery("SELECT SERVICE_ID" +
                                             " FROM NEIL_DERBY" +
                                             " WHERE SERVER_NAME='" + "server1" +
                                             "' AND LOG_ID=" + "1");
            while (rsBasic.next())
            {
                serviceID = rsBasic.getInt("SERVICE_ID");
            }
            System.out.println("XAFLOWSERVLET: NEIL_DERBY SERVICE_ID is " + serviceID);

            // Execute a Query against second connection, to check the value of the service_id
            System.out.println("create a statement");
            Statement stmtBasic2 = con2.createStatement();
            ResultSet rsBasic2 = null;

            System.out.println("Execute a query to check the value of the service_id in the second database");
            rsBasic2 = stmtBasic2.executeQuery("SELECT SERVICE_ID" +
                                               " FROM NEIL_DERBY2" +
                                               " WHERE SERVER_NAME='" + "server1" +
                                               "' AND LOG_ID=" + "1");

            while (rsBasic2.next())
            {
                serviceID2 = rsBasic2.getInt("SERVICE_ID");
            }
            System.out.println("XAFLOWSERVLET: NEIL_DERBY2 SERVICE_ID is " + serviceID2);

        } catch (Exception e)
        {
            System.out.println("XAFLOWSERVLET: Exception thrown when initializing: " + e);
            e.printStackTrace();

            throw new ServletException(e);
        }

        final PrintWriter out = response.getWriter();
        out.println("checkRecXAFlow001 COMPLETED SUCCESSFULLY expecting service id values 20 and 23, values were " + serviceID + " " + serviceID2);
        out.close();
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
     * This method is used to setup a connection to the DB.
     */
    public void setupRecXAFlow001(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        UserTransaction ut = null;
        final PrintWriter out = response.getWriter();
        out.println("Transaction started");
        try
        {
            // Do the lookups on the DS
            final InitialContext ctx = new InitialContext();
            System.out.println("XAFLOWSERVLET: Context is: " + ctx.toString());
            DataSource firstDS = (DataSource) ctx.lookup("java:comp/env/jdbc/myDataSource");
            System.out.println("XAFLOWSERVLET: Have looked up DS: " + firstDS);

            DataSource secondDS = (DataSource) ctx.lookup("java:comp/env/jdbc/secondDataSource");
            System.out.println("XAFLOWSERVLET: Have looked up DS: " + secondDS);

            // Get connection to database via first datasource
            Connection con1 = firstDS.getConnection();
            System.out.println("XAFLOWSERVLET: Got connection: " + con1);
            DatabaseMetaData mdata = con1.getMetaData();
            System.out.println("XAFLOWSERVLET: Got metadata: " + mdata);
            String dbName = mdata.getDatabaseProductName();
            String dbVersion = mdata.getDatabaseProductVersion();
            System.out.println("XAFLOWSERVLET: You are now connected to " + dbName + ", version " + dbVersion);

            // Get connection to database via second datasource
            Connection con2 = secondDS.getConnection();
            System.out.println("XAFLOWSERVLET: Got connection: " + con2);
            DatabaseMetaData mdata2 = con2.getMetaData();
            System.out.println("XAFLOWSERVLET: Got metadata: " + mdata2);
            String dbName2 = mdata2.getDatabaseProductName();
            String dbVersion2 = mdata2.getDatabaseProductVersion();
            System.out.println("XAFLOWSERVLET: You are now connected to " + dbName2 + ", version " + dbVersion2);

            // Start with a clean sheet, drop table if its already there
            System.out.println("create a statement");
            Statement stmtBasic = con1.createStatement();

            try
            {
                System.out.println("Drop existing table");
                stmtBasic.executeUpdate("DROP TABLE NEIL_DERBY");
            } catch (Exception e)
            {
                //Swallow this exception, assuming the table simply wasn't there
                System.out.println("Caught exception when dropping table NEIL_DERBY - " + e);
            }

            // Create the table
            Statement stmt2 = con1.createStatement();

            stmt2.executeUpdate("CREATE TABLE NEIL_DERBY( " +
                                "SERVER_NAME VARCHAR(128), " +
                                "SERVICE_ID SMALLINT, " +
                                "LOG_ID SMALLINT , " +
                                "RU_ID BIGINT, " +
                                "RUSECTION_ID BIGINT, " +
                                "RUSECTION_DATA_INDEX SMALLINT, " +
                                "DATA LONG VARCHAR FOR BIT DATA) ");

            System.out.println("Have created the table - insert special row");
            PreparedStatement specStatement = con1.prepareStatement("INSERT INTO NEIL_DERBY " +
                                                                    "(SERVER_NAME, SERVICE_ID, LOG_ID, RU_ID, RUSECTION_ID, RUSECTION_DATA_INDEX, DATA)" +
                                                                    " VALUES (?,?,?,?,?,?,?)");
            specStatement.setString(1, "server1");
            specStatement.setShort(2, (short) 1);
            specStatement.setShort(3, (short) 1);
            specStatement.setLong(4, -1);
            specStatement.setLong(5, 1);
            specStatement.setShort(6, (short) 1);
            byte buf[] = new byte[2];
            specStatement.setBytes(7, buf);
            int ret = specStatement.executeUpdate();
            System.out.println("Have inserted SPECIAL ROW with return: " + ret);

            stmt2.close();
            con1.commit();

            // Drop second table, if its already there
            System.out.println("create a statement");
            Statement stmtBasic2 = con2.createStatement();

            try
            {
                System.out.println("Drop existing table");
                stmtBasic2.executeUpdate("DROP TABLE NEIL_DERBY2");
            } catch (Exception e)
            {
                //Swallow this exception, assuming the table simply wasn't there
                System.out.println("Caught exception when dropping table NEIL_DERBY2 - " + e);
            }

            // Create Table
            stmt2 = con2.createStatement();

            stmt2.executeUpdate("CREATE TABLE NEIL_DERBY2( " +
                                "SERVER_NAME VARCHAR(128), " +
                                "SERVICE_ID SMALLINT, " +
                                "LOG_ID SMALLINT , " +
                                "RU_ID BIGINT, " +
                                "RUSECTION_ID BIGINT, " +
                                "RUSECTION_DATA_INDEX SMALLINT, " +
                                "DATA LONG VARCHAR FOR BIT DATA) ");

            System.out.println("Have created the table - insert special row");
            specStatement = con2.prepareStatement("INSERT INTO NEIL_DERBY2 " +
                                                  "(SERVER_NAME, SERVICE_ID, LOG_ID, RU_ID, RUSECTION_ID, RUSECTION_DATA_INDEX, DATA)" +
                                                  " VALUES (?,?,?,?,?,?,?)");
            specStatement.setString(1, "server1");
            specStatement.setShort(2, (short) 1);
            specStatement.setShort(3, (short) 1);
            specStatement.setLong(4, -1);
            specStatement.setLong(5, 1);
            specStatement.setShort(6, (short) 1);

            buf = new byte[2];
            specStatement.setBytes(7, buf);

            ret = specStatement.executeUpdate();
            System.out.println("Have inserted SPECIAL ROW with return: " + ret);

            stmt2.close();
            con2.commit();

            // Now we have the tables, we can do some work against them
            // Look up the User Transaction
            ut = (UserTransaction) ctx.lookup("java:comp/UserTransaction");
            System.out.println("XAFLOWSERVLET: Have looked up UT: " + ut);

            // Start a new transaction
            ut.begin();
            System.out.println("XAFLOWSERVLET: Start Transaction");

            // Execute an update
            System.out.println("XAFLOWSERVLET: Execute UPDATE against FIRST DERBY");

            PreparedStatement ps = con1.prepareStatement("UPDATE NEIL_DERBY SET SERVICE_ID=20 WHERE SERVER_NAME=?");
            ps.setString(1, "server1");
            System.out.println("XAFLOWSERVLET: make executeUpdate call");
            ps.executeUpdate();
            System.out.println("XAFLOWSERVLET: close prepared statement");
            ps.close();

            System.out.println("XAFLOWSERVLET: FIRST DERBY UPDATE has finished");

            // Execute an update
            System.out.println("XAFLOWSERVLET: Execute SECOND DERBY UPDATE");

            PreparedStatement ps2 = con2.prepareStatement("UPDATE NEIL_DERBY2 SET SERVICE_ID=23 WHERE SERVER_NAME=?");
            ps2.setString(1, "server1");
            System.out.println("XAFLOWSERVLET: make second executeUpdate call");
            ps2.executeUpdate();
            System.out.println("XAFLOWSERVLET: close second prepared statement");
            ps2.close();

            System.out.println("XAFLOWSERVLET: SECOND DERBY UPDATE has finished");
            out.println("Preparing to commit transaction");
        } catch (Exception e)
        {
            System.out.println("XAFLOWSERVLET: Exception thrown when initializing: " + e);
            e.printStackTrace();

            throw new ServletException(e);
        } finally
        {
            if (ut != null)
            {
                try
                {
                    System.out.println("XAFLOWSERVLET: Drive COMMIT processing");
                    ut.commit();
                } catch (Exception e)
                {
                    throw new ServletException(e);
                }
            }
        }
        out.println("XAFLOWSERVLET: TRANSACTION COMPLETED SUCCESSFULLY");
        out.close();
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    public static final String SUCCESS_MESSAGE = "COMPLETED SUCCESSFULLY";

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String test = request.getParameter("test");
        PrintWriter out = response.getWriter();
        System.out.println("Starting test " + test + "<br>");
        out.println("Starting test " + test +);
        try {
            getClass().getMethod(test, HttpServletRequest.class, HttpServletResponse.class).invoke(this, request, response);
            out.println(test + " " + SUCCESS_MESSAGE);
        } catch (Throwable x) {
            if (x instanceof InvocationTargetException)
                x = x.getCause();
            out.println("<pre>ERROR in " + test + ":");
            x.printStackTrace(out);
            out.println("</pre>");
        }
    }

    public static String printStatus(int status)
    {
        switch (status)
        {
            case Status.STATUS_ACTIVE:
                return "Status.STATUS_ACTIVE";
            case Status.STATUS_COMMITTED:
                return "Status.STATUS_COMMITTED";
            case Status.STATUS_COMMITTING:
                return "Status.STATUS_COMMITTING";
            case Status.STATUS_MARKED_ROLLBACK:
                return "Status.STATUS_MARKED_ROLLBACK";
            case Status.STATUS_NO_TRANSACTION:
                return "Status.STATUS_NO_TRANSACTION";
            case Status.STATUS_PREPARED:
                return "Status.STATUS_PREPARED";
            case Status.STATUS_PREPARING:
                return "Status.STATUS_PREPARING";
            case Status.STATUS_ROLLEDBACK:
                return "Status.STATUS_ROLLEDBACK";
            case Status.STATUS_ROLLING_BACK:
                return "Status.STATUS_ROLLING_BACK";
            default:
                return "Status.STATUS_UNKNOWN";
        }
    }
}
