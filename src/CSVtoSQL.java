import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class CSVtoSQL {

    private static Connection connection;
    private static Properties propiedades;

    public static void main(String[] args) {
        propiedades = new Properties();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            propiedades.load(new FileInputStream(new File("config.ini")));
            connection = DriverManager.getConnection(propiedades.getProperty("url"), propiedades.getProperty("user"),
                    propiedades.getProperty("password"));
        
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
