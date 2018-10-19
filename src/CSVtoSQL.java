import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

public class CSVtoSQL {

    private static LinkedList<String[]> tablaDatos;
    private static HashMap<Integer, String> magnitudes;
    private static HashMap<Integer, String> estaciones;

    private static Connection connection;
    private static Properties propiedades;

    public static void insertarBBDD(LinkedList<String[]> tablaDatos) {
        //Creación
        // language=SQL
        String queryCrearTablas = "CREATE TABLE contaminacion (" +
                "MAGNITUD VARCHAR(50)," +
                "ESTACION VARCHAR(50)," +
                "FECHA DATE";
        String[] primeraFila = tablaDatos.get(0);
        for (int i = 8; i < primeraFila.length; i++) {
            queryCrearTablas += ", " + primeraFila[i] + " VARCHAR(50)";
        }
        queryCrearTablas += ");";

        //Inserción
        // language=SQL
        String queryInserción = "INSERT INTO contaminacion (MAGNITUD, ESTACION, FECHA, H01, V01, H02, V02, H03, V03, H04, V04, H05, V05, H06, V06, H07, V07, H08, V08, H09, V09, H10, V10, H11, V11, H12, V12, H13, V13, H14, V14, H15, V15, H16, V16, H17, V17, H18, V18, H19, V19, H20, V20, H21, V21, H22, V22, H23, V23, H24, V24) " +
                "VALUES ";
        boolean primero = true;
        int x = 0;
        for (String[] fila : tablaDatos) {
            if (x > 0) {
                if (!primero)
                    queryInserción += ", ";
                else
                    primero = false;
                queryInserción += "(";
                queryInserción += "'" + magnitudes.get(Integer.parseInt(fila[3])) + "', '" + estaciones.get(Integer.parseInt(fila[4].substring(0, 8))) + "', ";
                queryInserción += "'" + fila[5] + "-" + fila[6] + "-" + fila[7] + "'";
                for (int i = 8; i < fila.length; i++) {
                    queryInserción += ", '" + fila[i] + "'";
                }
                queryInserción += ")";
            } else {
                x++;
            }
        }
        queryInserción += ";";
        System.out.println(queryInserción);
        try {
            // language=SQL
            connection.prepareStatement("DROP TABLE IF EXISTS contaminacion;").executeUpdate();
            PreparedStatement statement = connection.prepareStatement(queryCrearTablas);
            statement.executeUpdate();
            connection.prepareStatement(queryInserción).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static LinkedList<String[]> leerTabla() {
        /*try {
            URL url = new URL("http://www.mambiente.munimadrid.es/opendata/horario.csv");
            ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
            FileOutputStream fileOutputStream = new FileOutputStream("./horario.csv");
            fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        } catch (Exception e) {
            System.out.println("No se ha podido descargar el archivo.");
        }*/
        BufferedReader bufferDatos = null;
        LinkedList<String[]> tabla = new LinkedList<>();
        magnitudes = new HashMap<>();
        estaciones = new HashMap<>();
        try {
            String strLinea;
            bufferDatos = new BufferedReader(new FileReader("horario.csv"));
            BufferedReader bufferMagnitudes = new BufferedReader(new FileReader("magnitudes.csv"));
            BufferedReader bufferEstaciones = new BufferedReader(new FileReader("estaciones.csv"));
            while ((strLinea = bufferDatos.readLine()) != null) {
                tabla.add(strLinea.split(";"));
            }
            while ((strLinea = bufferMagnitudes.readLine()) != null) {
                String[] magnitud = strLinea.split(";");
                magnitudes.put(Integer.parseInt(magnitud[0]), magnitud[1]);
            }
            while ((strLinea = bufferEstaciones.readLine()) != null) {
                String[] estacion = strLinea.split(";");
                estaciones.put(Integer.parseInt(estacion[0]), estacion[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tabla;
    }

    public static void main(String[] args) {
        propiedades = new Properties();
        tablaDatos = leerTabla();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            propiedades.load(new FileInputStream(new File("config.ini")));
            connection = DriverManager.getConnection(propiedades.getProperty("url"), propiedades.getProperty("user"),
                    propiedades.getProperty("password"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        insertarBBDD(tablaDatos);
        new String();
    }

}
