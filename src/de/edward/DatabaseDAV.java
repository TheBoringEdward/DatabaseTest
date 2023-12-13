package de.edward;

        import java.time.Instant;
        import java.util.Date;
        import java.util.Properties;

        import java.sql.Connection;
        import java.sql.PreparedStatement;
        import java.sql.ResultSet;
        import java.sql.SQLException;
        import java.sql.Timestamp;

/**
 * Die Haupt- und Main-Klasse dieses {@link DatabaseDAV Test-Projekts}, mit dessen Hilfe eine stabile Verbindung zu
 * einer Datenbank hergestellt werden soll.
 */
public final class DatabaseDAV {

    //<editor-fold desc="CONSTANTS">
    /** Der Pool-Name, mit dem der {@link DatabaseHandler} initialisiert wird. */
    private static final String POOL_NAME = "TestDB";
    /** Die Adresse, unter der die Datenbank angesprochen wird. */
    private static final String JDBC_URL = "jdbc:mariadb://localhost:3306/info";
    /** Der Nutzer, mit dem auf die Datenbank zugegriffen wird. */
    private static final String USER = "edward";
    /** Das Passwort, welches zu dem {@code USER} gehört. */
    private static final String PASSWORD = "admin"; //highest of security standards!!!
    //</editor-fold>


    //<editor-fold desc="STATIC FIELDS">
    /** Der {@link DatabaseHandler}, mit der die Verbindung zu der Datenbank hergestellt wird. */
    private static DatabaseHandler databaseHandler;
    //</editor-fold>


    //<editor-fold desc="main">

    /**
     * Die Main-Methode dieser Anwendung, welche als erstes aufgerufen wird und die Schnittstelle zu der JRE darstellt.
     *
     * @param args Die Argumente, die beim Ausführen dieser Anwendung übergeben wird.
     */
    public static void main(final String[] args) {
        // save properties to instance database-handler
        final Properties databaseProperties = new Properties();
        databaseProperties.put("jdbcUrl", JDBC_URL);
        databaseProperties.put("dataSource.user", USER);
        databaseProperties.put("dataSource.password", PASSWORD);

        // create database-handler
        databaseHandler = new DatabaseHandler(
                POOL_NAME,
                databaseProperties
        );

        // create table
        createDAVTable();

        // insert values
        try {
            int i = 0;
            insertValues("Atkins", "Käthe", "w", 2001-1-01, "c");
            Thread.sleep(100);
            insertValues("fatchi", "now/shall", "adverb", "marker for imperative mood");
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        // get the newest name
        System.out.println(getNewestEntry());
    }
    //</editor-fold>


    /**
     * Gibt den {@link DatabaseHandler} zurück, mit dem eine stabile Verbindung zur Datenbank hergestellt wurde.
     * @return Der {@link DatabaseHandler}, mit dem eine stabile Verbindung zur Datenbank hergestellt wurde.
     */
    public static DatabaseHandler getDatabaseHandler() {
        return databaseHandler;
    }


    //<editor-fold desc="utility">

    /**
     * Erzeugt eine Test-Tabelle
     */
    private static void createDAVTable() {
        try (final Connection conn = getDatabaseHandler().getConnection()) {
            final PreparedStatement stmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS DAVTable("
                            + "name VARCHAR(255) PRIMARY KEY, "
                            + "vorname VARCHAR(255), "
                            + "geschlecht VARCHAR(1), "
                            + "geburtsdatum DATE, "
                            + "disziplin VARCHAR(1), "
                            + "date DATETIME(1)"
                            + ")" //I'm not entirely sure, I know what i'm doing...
            );

            stmt.executeUpdate();
        } catch (final SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Fügt einen neuen Datensatz hinzu, welcher aus einem Namen, einem Alter und einem Zeitpunkt besteht, wobei der
     * Zeitpunkt nicht mit übergeben wird, da immer der aktuelle Zeitpunkt gewählt wird. Sollte der Name bereits
     * verwendet werden, wird einfach nur der Zeitstempel aktualisiert.
     *
     * @param name Der Name, welcher für diesen Eintrag genutzt werden soll.
     * @param vorname  Das Alter, welches für diesen Eintrag genutzt werden soll.
     */
    private static void insertValues(
            final String name,
            final String vorname,
            final String geschlecht,
            final Date geburtsdatum,
            final String disziplin
    ) {
        try (final Connection conn = getDatabaseHandler().getConnection()) {
            final PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO DAVTable (name, vorname, geschlecht, geburtsdatum, date) VALUES (@name:=?, @vorname:=?, @geschlecht:=?, @geburtsdatum:=?, @disziplin:=?, @date:=?) "
                            + "ON DUPLICATE KEY UPDATE date=@date, disziplin=@disziplin, geburtsdatum=@geburtsdatum, geschlecht=@geschlecht, vorname=@vorname"
            );

            stmt.setString(1, name);
            stmt.setString(2, vorname);
            stmt.setString(3, geschlecht);
            stmt.setTimestamp(4, geburtsdatum);
            stmt.setString(5, disziplin);
            stmt.setTimestamp(6, Timestamp.from(Instant.now()));

            stmt.executeUpdate();
        } catch (final SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gibt den Namen zurück, der zuletzt eingefügt wurde in der Datenbank.
     *
     * @return Der Name, der zuletzt eingefügt wurde in der Datenbank.
     */
    private static String getNewestEntry() {
        try (final Connection conn = getDatabaseHandler().getConnection()) {
            final PreparedStatement stmt = conn.prepareStatement(
                    "SELECT name as name FROM DAVTable WHERE date IS NOT NULL ORDER BY date DESC"
            );

            final ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getString("name");
            }
        } catch (final SQLException e) {
            e.printStackTrace();
        }

        return "";
    }
    //</editor-fold>

}
