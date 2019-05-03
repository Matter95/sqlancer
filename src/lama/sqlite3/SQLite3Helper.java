package lama.sqlite3;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import lama.schema.Schema;
import lama.schema.Schema.Table;

public class SQLite3Helper {

	public static int getNrRows(Connection con, Table table) throws SQLException {
		try (Statement s = con.createStatement()) {
			try (ResultSet query = s.executeQuery("SELECT COUNT(*) FROM " + table.getName())) {
				query.next();
				return query.getInt(1);
			}
		}
	}

	public static void dropTable(Connection con, Table table) throws SQLException {
		try (Statement s = con.createStatement()) {
			s.execute("DROP TABLE IF EXISTS " + table.getName());
		}
	}

	public static void deleteAllTables(Connection con) throws SQLException {
		Schema previousSchema = Schema.fromConnection(con);
		for (Table t : previousSchema.getDatabaseTables()) {
			dropTable(con, t);
		}
	}

}