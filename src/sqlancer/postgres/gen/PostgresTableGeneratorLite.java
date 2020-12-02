package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.sqlite3.gen.SQLite3Common;

public class PostgresTableGeneratorLite extends PostgresTableGenerator {

    private final String tableName;
    private boolean columnCanHavePrimaryKey;
    private boolean columnHasPrimaryKey;
    private final StringBuilder sb = new StringBuilder();
    private boolean isTemporaryTable;
    private final PostgresSchema newSchema;
    private final List<PostgresColumn> columnsToBeAdded = new ArrayList<>();
    protected final ExpectedErrors errors = new ExpectedErrors();
    private final PostgresTable table;
    private final boolean generateOnlyKnown;
    private final PostgresGlobalState globalState;

    public PostgresTableGeneratorLite(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
    	super(tableName, newSchema, generateOnlyKnown, globalState);
        this.tableName = tableName;
        this.newSchema = newSchema;
        this.generateOnlyKnown = generateOnlyKnown;
        this.globalState = globalState;
        table = new PostgresTable(tableName, columnsToBeAdded, null, null, null, false, false);
        
    }

    public static Query generate(String tableName, PostgresSchema newSchema, boolean generateOnlyKnown,
            PostgresGlobalState globalState) {
        return new PostgresTableGeneratorLite(tableName, newSchema, generateOnlyKnown, globalState).generate();
    }

    Query generate() {
        columnCanHavePrimaryKey = true;
        sb.append("CREATE");
        sb.append(" TABLE");
        sb.append(" ");
        sb.append(tableName);
        createStandard();
        return new QueryAdapter(sb.toString(), errors, true);
    }

    private void createStandard() throws AssertionError {
        sb.append("(");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String name = SQLite3Common.createColumnName(i);
            createColumn(name);
        }

        sb.append(")");
        PostgresCommon.generateWith(sb, globalState, errors);
        if (Randomly.getBoolean() && isTemporaryTable) {
            sb.append(" ON COMMIT ");
            sb.append(Randomly.fromOptions("PRESERVE ROWS", "DELETE ROWS", "DROP"));
            sb.append(" ");
        }
    }

    private void createColumn(String name) throws AssertionError {
        sb.append(name);
        sb.append(" ");
        PostgresDataType type = PostgresDataType.INT;
        boolean serial = PostgresCommon.appendDataType(type, sb, true, generateOnlyKnown, globalState.getCollates());
        PostgresColumn c = new PostgresColumn(name, type);
        c.setTable(table);
        columnsToBeAdded.add(c);
        sb.append(" ");
        //TODO:: back to random, right now it guarantees a check constraint
        if (true) {
            createColumnConstraint(type, serial);
        }
    }

    private enum ColumnConstraint {
       CHECK
    };

    private void createColumnConstraint(PostgresDataType type, boolean serial) {
        List<ColumnConstraint> constraintSubset = new ArrayList<ColumnConstraint>();
        constraintSubset.add(ColumnConstraint.CHECK);
        
        for (ColumnConstraint c : constraintSubset) {
            sb.append(" ");
            switch (c) {
            case CHECK:
                sb.append("CHECK (");
                sb.append(PostgresVisitor.asString(PostgresExpressionGeneratorLite.generateExpression(globalState,
                        columnsToBeAdded, PostgresDataType.BOOLEAN)));
                sb.append(")");
                
                errors.add("out of range");
                break;
            default:
                throw new AssertionError(sb);
            }
        }
    }

}
