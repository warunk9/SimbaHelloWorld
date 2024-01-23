
import com.simba.googlebigquery.jdbc.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.simba.googlebigquery.jdbc.DataSource;

public class App {


    private static Connection connectViaDS() throws Exception {



        String KEYFILE = "/Users/warunk/Downloads/gcp-sandbox-3-393305-d565c251d5e5.json";   //todo: to be set according to the gcp project
        String EMAIL = "test-simba-jdbc@gcp-sandbox-3-393305.iam.gserviceaccount.com"; // todo: service account email to be set accordingly
        String PROJECT = "gcp-sandbox-3-393305";  // todo: set project id 
        String CONNECTION_URL = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;" ;
    
        Connection connection = null;
        DataSource ds = new DataSource();
        ds.setURL(CONNECTION_URL);
        ds.setProjectId(PROJECT);
        ds.setOAuthType(0); // Service Authentication, there are different authentication types available
        ds.setOAuthServiceAcctEmail(EMAIL);
        ds.setOAuthPvtKeyFilePath(KEYFILE);
        connection = ds.getConnection();
        return connection;
}
    public static void main(String[] args) throws Exception 
    {
        System.out.println("Hello world!");

        try (Connection conn = connectViaDS();
             Statement stmt = conn.createStatement()) {

            String query = "SELECT id, userid,session_id FROM gcp-sandbox-3-393305.test_simba_jdbc.stg_events limit 5";
            // 
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                // Process the result set (e.g., print the values)
                System.out.println("id: " + rs.getString(1)); 
                System.out.println("userid: " + rs.getString(2)); 
                System.out.println("session_id: " + rs.getString(3)); 
            }

        } catch (Exception e) {
            System.err.println("Error during query execution: " + e.getMessage());
        }




    }
}