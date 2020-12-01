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
        errors.add("invalid input syntax for");
        errors.add("is not unique");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("cannot create partitioned table as inheritance child");
        errors.add("cannot cast");
        errors.add("ERROR: functions in index expression must be marked IMMUTABLE");
        errors.add("functions in partition key expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not exist for access method");
        errors.add("does not accept data type");
        errors.add("but default expression is of type text");
        errors.add("has pseudo-type unknown");
        errors.add("no collation was derived for partition key column");
        errors.add("inherits from generated column but specifies identity");
        errors.add("inherits from generated column but specifies default");
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonTableErrors(errors);
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
        if (Randomly.getBoolean()) {
            errors.add("constraints on temporary tables may reference only temporary tables");
            errors.add("constraints on unlogged tables may reference only permanent or unlogged tables");
            errors.add("constraints on permanent tables may reference only permanent tables");
            errors.add("cannot be implemented");
            errors.add("there is no unique constraint matching given keys for referenced table");
            errors.add("cannot reference partitioned table");
            errors.add("unsupported ON COMMIT and foreign key combination");
            errors.add("ERROR: invalid ON DELETE action for foreign key constraint containing generated column");
            errors.add("exclusion constraints are not supported on partitioned tables");
            PostgresCommon.addTableConstraints(columnHasPrimaryKey, sb, table, globalState, errors);
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
        //TODO:: back to random
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
