package sqlancer.tidb.gen;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public class TiDBInsertGenerator {
	
	private final TiDBGlobalState globalState;
	private final Set<String> errors = new HashSet<>();

	public TiDBInsertGenerator(TiDBGlobalState globalState) {
		this.globalState = globalState;
		errors.add("Duplicate entry");
		errors.add("cannot be null");
		errors.add("doesn't have a default value");
	}

	public static Query getQuery(TiDBGlobalState globalState) throws SQLException {
		return new TiDBInsertGenerator(globalState).get();
	}

	private Query get() {
		TiDBTable table = globalState.getSchema().getRandomTable();
		StringBuilder sb = new StringBuilder();
		sb.append(Randomly.fromOptions("INSERT", "REPLACE"));
		sb.append(" INTO ");
		sb.append(table.getName());
		if (Randomly.getBoolean()) {
			sb.append(" VALUES ");
			List<TiDBColumn> columns = table.getColumns();
			insertColumns(sb, columns);
		} else {
			List<TiDBColumn> columnSubset = table.getRandomNonEmptyColumnSubset();
			sb.append("(");
			sb.append(columnSubset.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(") VALUES ");
			insertColumns(sb, columnSubset);
		}
		return new QueryAdapter(sb.toString(), errors);
	}

	private void insertColumns(StringBuilder sb, List<TiDBColumn> columns) {
		for (int nrRows = 0; nrRows < Randomly.smallNumber() + 1; nrRows++) {
			if (nrRows != 0) {
				sb.append(", ");
			}
			sb.append("(");
			for (int nrColumn = 0; nrColumn < columns.size(); nrColumn++) {
				if (nrColumn != 0) {
					sb.append(", ");
				}
				insertValue(sb, columns.get(nrColumn));
			}
			sb.append(")");
		}
	}

	private void insertValue(StringBuilder sb, TiDBColumn tiDBColumn) {
		// TODO: make dependent on column
		if (Randomly.getBooleanWithRatherLowProbability()) {
			sb.append("NULL");
		} else {
			sb.append(Randomly.getNotCachedInteger(0, 100));
			errors.add("out of range");
		}
	}

}
