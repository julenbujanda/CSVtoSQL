import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;

public class CSVtoSQL {

    private static LinkedList<String[]> tablaDatos;
    private static LinkedList<String[]> contaminantes;
    private static LinkedList<String[]> estaciones;

    private static Connection connection;
    private static Properties propiedades;
    private static Scanner scanner;

    public static void crearTablas() {
        // language=SQL
        String queryCrearTablaContaminacion = "CREATE TABLE contaminacion (" +
                "MAGNITUD INT," +
                "ESTACION INT," +
                "FECHA DATE," +
                "H01 VARCHAR(50),V01 VARCHAR(50), H02 VARCHAR(50), V02 VARCHAR(50), H03 VARCHAR(50), V03 VARCHAR(50), H04 VARCHAR(50), V04 VARCHAR(50), H05 VARCHAR(50), V05 VARCHAR(50), H06 VARCHAR(50), V06 VARCHAR(50), H07 VARCHAR(50), V07 VARCHAR(50), H08 VARCHAR(50), V08 VARCHAR(50), H09 VARCHAR(50), V09 VARCHAR(50), H10 VARCHAR(50), V10 VARCHAR(50), H11 VARCHAR(50), V11 VARCHAR(50), H12 VARCHAR(50), V12 VARCHAR(50), H13 VARCHAR(50), V13 VARCHAR(50), H14 VARCHAR(50), V14 VARCHAR(50), H15 VARCHAR(50), V15 VARCHAR(50), H16 VARCHAR(50), V16 VARCHAR(50), H17 VARCHAR(50), V17 VARCHAR(50), H18 VARCHAR(50), V18 VARCHAR(50), H19 VARCHAR(50), V19 VARCHAR(50), H20 VARCHAR(50), V20 VARCHAR(50), H21 VARCHAR(50), V21 VARCHAR(50), H22 VARCHAR(50), V22 VARCHAR(50), H23 VARCHAR(50), V23 VARCHAR(50), H24 VARCHAR(50), V24 VARCHAR(50)," +
                "FOREIGN KEY (MAGNITUD) REFERENCES contaminantes(ID)," +
                "FOREIGN KEY (ESTACION) REFERENCES estaciones(ID));";
        // language=SQL
        String queryCrearTablaEstaciones = "CREATE TABLE estaciones (" +
                "ID INT PRIMARY KEY," +
                "NOMBRE VARCHAR(50));";
        // language=SQL
        String queryCrearTablaContaminantes = "CREATE TABLE contaminantes (" +
                "ID INT PRIMARY KEY," +
                "NOMBRE VARCHAR(50));";
        try {
            // language=SQL
            connection.prepareStatement("DROP TABLE IF EXISTS contaminacion;").executeUpdate();
            connection.prepareStatement("DROP TABLE IF EXISTS estaciones;").executeUpdate();
            connection.prepareStatement("DROP TABLE IF EXISTS contaminantes;").executeUpdate();
            connection.prepareStatement(queryCrearTablaEstaciones).executeUpdate();
            connection.prepareStatement(queryCrearTablaContaminantes).executeUpdate();
            connection.prepareStatement(queryCrearTablaContaminacion).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void insertarEstaciones() {
        // language=SQL
        String queryInsercionEstaciones = "INSERT INTO estaciones (ID, NOMBRE) VALUES ";
        boolean primero = true;
        for (String[] fila : estaciones) {
            if (!primero)
                queryInsercionEstaciones += ", ";
            else
                primero = false;
            queryInsercionEstaciones += "(" + fila[0] + ", '" + fila[1] + "')";
        }
        try {
            connection.prepareStatement(queryInsercionEstaciones).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void insertarContaminantes() {
        // language=SQL
        String queryInsercionContaminantes = "INSERT INTO contaminantes (ID, NOMBRE) VALUES ";
        boolean primero = true;
        for (String[] fila : contaminantes) {
            if (!primero)
                queryInsercionContaminantes += ", ";
            else
                primero = false;
            queryInsercionContaminantes += "(" + fila[0] + ", '" + fila[1] + "')";
        }
        try {
            connection.prepareStatement(queryInsercionContaminantes).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void insertarDatos() {
        //Creación

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
                queryInserción += fila[3] + ", " + fila[4].substring(0, 8) + ", ";
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
        try {
            connection.prepareStatement(queryInserción).executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void leerTablas() {
        /*
        Intenta descargar el archivo para obtener datos en tiempo real,
         si no se consigue se utilizará el último guardado
         */
        try {
            URL url = new URL("http://www.mambiente.munimadrid.es/opendata/horario.csv");
            ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
            FileOutputStream fileOutputStream = new FileOutputStream("./horario.csv");
            fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        } catch (Exception e) {
            System.out.println("No se ha podido descargar el archivo.");
        }
        tablaDatos = new LinkedList<>();
        contaminantes = new LinkedList<>();
        estaciones = new LinkedList<>();
        try {
            String strLinea;
            BufferedReader bufferDatos = new BufferedReader(new FileReader("horario.csv"));
            BufferedReader bufferMagnitudes = new BufferedReader(new FileReader("magnitudes.csv"));
            BufferedReader bufferEstaciones = new BufferedReader(new FileReader("estaciones.csv"));
            while ((strLinea = bufferDatos.readLine()) != null) {
                tablaDatos.add(strLinea.split(";"));
            }
            while ((strLinea = bufferMagnitudes.readLine()) != null) {
                contaminantes.add(strLinea.split(";"));
            }
            while ((strLinea = bufferEstaciones.readLine()) != null) {
                estaciones.add(strLinea.split(";"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int mostrarEstaciones() {
        System.out.println("Escoger estación:");
        int seleccion = 0;
        HashMap<Integer, Integer> ids = new HashMap<>();
        try {
            ResultSet resultSetEstaciones = connection.prepareStatement(
                    "SELECT ID, NOMBRE FROM estaciones, contaminacion WHERE estaciones.ID = contaminacion.ESTACION GROUP BY ID;").executeQuery();
            int num = 0;
            while (resultSetEstaciones.next()) {
                System.out.println(++num + ") " + resultSetEstaciones.getString(2));
                ids.put(num, resultSetEstaciones.getInt(1));
            }
            seleccion = scanner.nextInt();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ids.get(seleccion);
    }

    public static int mostrarContaminantes(int estacion) {
        System.out.println("Escoger contaminante:");
        int seleccion = 0;
        HashMap<Integer, Integer> ids = new HashMap<>();
        try {
            ResultSet resultSetContaminantes = connection.prepareStatement(
                    "SELECT ID, NOMBRE FROM contaminantes, contaminacion WHERE contaminacion.MAGNITUD = contaminantes.ID AND ESTACION = " + estacion + " GROUP BY ID;").executeQuery();
            int num = 0;
            while (resultSetContaminantes.next()) {
                System.out.println(++num + ") " + resultSetContaminantes.getString(2));
                ids.put(num, resultSetContaminantes.getInt(1));
            }
            seleccion = scanner.nextInt();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ids.get(seleccion);
    }

    public static void mostrarResultados(int estacion, int contaminante) {
        System.out.println("Resultados:");
        try {
            ResultSet resultSetDatos = connection.prepareStatement("SELECT H01, V01, H02, V02, H03, V03, H04, V04, H05, V05, H06, V06, H07, V07, H08, V08, H09, V09, H10, V10, H11, V11, H12, V12, H13, V13, H14, V14, H15, V15, H16, V16, H17, V17, H18, V18, H19, V19, H20, V20, H21, V21, H22, V22, H23, V23, H24, V24 FROM contaminacion WHERE MAGNITUD = " + contaminante + " AND ESTACION = " + estacion + ";").executeQuery();
            ResultSetMetaData resultSetMetaData = resultSetDatos.getMetaData();
            int numColumnas = resultSetMetaData.getColumnCount();
            String salida = "";
            while (resultSetDatos.next()) {
                for (int i = 1; i <= numColumnas; i++) {
                    salida += resultSetMetaData.getColumnName(i) + ": " + resultSetDatos.getString(i) + "\n";
                }
            }
            System.out.println(salida);
        } catch (SQLException e) {

        }
    }

    public static void main(String[] args) {
        propiedades = new Properties();
        leerTablas();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            propiedades.load(new FileInputStream(new File("config.ini")));
            connection = DriverManager.getConnection(propiedades.getProperty("url"), propiedades.getProperty("user"),
                    propiedades.getProperty("password"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        crearTablas();
        insertarEstaciones();
        insertarContaminantes();
        insertarDatos();
        scanner = new Scanner(System.in);
        int estacion = mostrarEstaciones();
        int contaminante = mostrarContaminantes(estacion);
        mostrarResultados(estacion, contaminante);
    }

}
