package de.edward;

import java.sql.*;
import java.time.Instant;
import java.util.Properties;

import java.sql.Connection;
        import java.sql.PreparedStatement;
        import java.sql.ResultSet;
        import java.sql.SQLException;
        import java.sql.Timestamp;
        import java.time.Instant;
        import java.util.Properties;

/**
 * Die Haupt- und Main-Klasse dieses {@link DatabaseDictionary Test-Projekts}, mit dessen Hilfe eine stabile Verbindung zu
 * einer Datenbank hergestellt werden soll.
 */
public final class DatabaseDictionary {

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
        createdictionaryTable();

        // insert values
        try {
            int i = 0;
            insertValues("tchó", "person", "noun", "general definition for individual humans");
            Thread.sleep(100);
            insertValues("fatchi", "now/shall", "adverb", "marker for imperative mood");
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        // get the newest name
        System.out.println(getNewestWord());
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
    private static void createdictionaryTable() {
        try (final Connection conn = getDatabaseHandler().getConnection()) {
            final PreparedStatement stmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS dictionaryTable("
                            + "strangWord VARCHAR(255) PRIMARY KEY, "
                            + "engWord VARCHAR(255), "
                            + "type VARCHAR(255), "
                            + "definition VARCHAR(255), "
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
     * @param strangWord Der Name, welcher für diesen Eintrag genutzt werden soll.
     * @param engWord  Das Alter, welches für diesen Eintrag genutzt werden soll.
     */
    private static void insertValues(
            final String strangWord,
            final String engWord,
            final String type,
            final String definition
    ) {
        try (final Connection conn = getDatabaseHandler().getConnection()) {
            final PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO dictionaryTable (strangWord, engWord, type, definition, date) VALUES (@strangWord:=?, @engWord:=?, @type:=?, @definition:=?, @date:=?) "
                            + "ON DUPLICATE KEY UPDATE date=@date, definition=@definition, type=@type, engWord=@engWord"
            );

            stmt.setString(1, strangWord);
            stmt.setString(2, engWord);
            stmt.setString(3, type);
            stmt.setString(4, definition);
            stmt.setTimestamp(5, Timestamp.from(Instant.now()));

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
    private static String getNewestWord() {
        try (final Connection conn = getDatabaseHandler().getConnection()) {
            final PreparedStatement stmt = conn.prepareStatement(
                    "SELECT strangWord as strangWord FROM dictionaryTable WHERE date IS NOT NULL ORDER BY date DESC"
            );

            final ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getString("strangWord");
            }
        } catch (final SQLException e) {
            e.printStackTrace();
        }

        return "";
    }
    //</editor-fold>

}
