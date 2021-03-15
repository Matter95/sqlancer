package sqlancer.postgres.oracle;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresCompoundDataType;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.ast.PostgresPostfixText;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;
import sqlancer.postgres.ast.PostgresSelect.SelectType;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.tlp.PostgresTLPBase;

public class PostgresNoRECOracle extends NoRECBase<PostgresGlobalState> implements TestOracle {

    private final PostgresSchema s;
    private PostgresGlobalState globalState;

    public PostgresNoRECOracle(PostgresGlobalState globalState) {
        super(globalState);
        this.globalState = globalState;
        this.s = globalState.getSchema();
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException, IOException {
        FileWriter csvWriter = new FileWriter(globalState.getDmbsSpecificOptions().path, true);
        int secondCount = -1;
        try {
            PostgresTables randomTables = s.getRandomTableNonEmptyTables();
            List<PostgresColumn> columns = randomTables.getColumns();
            PostgresExpression randomWhereCondition;

            PostgresExpression column_0 = PostgresColumnValue.create(columns.get(0), null);
            PostgresExpression column_1 = PostgresColumnValue.create(columns.get(1), null);
            PostgresExpression column_2 = PostgresColumnValue.create(columns.get(2), null);

            switch (globalState.getDmbsSpecificOptions().fixedQueryNr) {
            case 0:
                switch (globalState.getDmbsSpecificOptions().queryDiff) {
                case 0:
                    // Create fixed Query t0.c0 > 0
                    randomWhereCondition = new PostgresBinaryComparisonOperation(column_0,
                            PostgresConstant.createIntConstant(0), PostgresBinaryComparisonOperator.GREATER);
                    break;
                case 1:
                    // Create fixed Query (t0.c0 > 0 AND t0.c0 < 10000)
                    randomWhereCondition = new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(0),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(10000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND);
                    break;
                case 2:
                    // Create fixed Query (t0.c0 > 5000 AND t0.c0 < 10000)
                    randomWhereCondition = new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(5000),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(10000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND);
                    break;
                default:
                    throw new AssertionError();
                }

                break;
            case 1:
                switch (globalState.getDmbsSpecificOptions().queryDiff) {
                case 0:
                    // Create fixed Query t0.c0 > 0 AND t0.c1 < 50000
                    randomWhereCondition = new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(0),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_1, PostgresConstant.createIntConstant(50000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND);

                    break;
                case 1:
                    // Create fixed Query (t0.c0 > 0 AND t0.c0 < 10000) AND (t0.c1 < 50000 AND t0.c1 > 40000)
                    randomWhereCondition = new PostgresBinaryLogicalOperation(new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(0),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(10000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND),

                            new PostgresBinaryLogicalOperation(new PostgresBinaryComparisonOperation(column_1,
                                    PostgresConstant.createIntConstant(50000), PostgresBinaryComparisonOperator.LESS),
                                    new PostgresBinaryComparisonOperation(column_1,
                                            PostgresConstant.createIntConstant(40000),
                                            PostgresBinaryComparisonOperator.GREATER),
                                    BinaryLogicalOperator.AND),
                            BinaryLogicalOperator.AND);
                    break;
                case 2:
                    // Create fixed Query (t0.c0 > 5000 AND t0.c0 < 10000) AND (t0.c1 < 50000 AND t0.c1 > 45000)
                    randomWhereCondition = new PostgresBinaryLogicalOperation(new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(5000),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(10000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND),

                            new PostgresBinaryLogicalOperation(new PostgresBinaryComparisonOperation(column_1,
                                    PostgresConstant.createIntConstant(50000), PostgresBinaryComparisonOperator.LESS),
                                    new PostgresBinaryComparisonOperation(column_1,
                                            PostgresConstant.createIntConstant(45000),
                                            PostgresBinaryComparisonOperator.GREATER),
                                    BinaryLogicalOperator.AND),
                            BinaryLogicalOperator.AND);
                    break;
                default:
                    throw new AssertionError();
                }

                break;
            case 2:
                switch (globalState.getDmbsSpecificOptions().queryDiff) {
                case 0:
                    // Create fixed Query t0.c0 > 0 AND t0.c1 < 50000 AND t0.c2 > -15000
                    randomWhereCondition = new PostgresBinaryLogicalOperation(new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(0),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_1, PostgresConstant.createIntConstant(50000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND),
                            new PostgresBinaryComparisonOperation(column_2, PostgresConstant.createIntConstant(-15000),
                                    PostgresBinaryComparisonOperator.GREATER),
                            BinaryLogicalOperator.AND);

                    break;
                case 1:

                    // Create fixed Query (t0.c0 > 0 AND t0.c0 < 10000) AND (t0.c1 < 50000 AND t0.c1 > 40000) AND (t0.c2
                    // > -15000 AND t0.c2 <= -5000)
                    randomWhereCondition = new PostgresBinaryLogicalOperation(new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(0),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(10000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND),

                            new PostgresBinaryLogicalOperation(
                                    new PostgresBinaryLogicalOperation(
                                            new PostgresBinaryComparisonOperation(column_1,
                                                    PostgresConstant.createIntConstant(50000),
                                                    PostgresBinaryComparisonOperator.LESS),
                                            new PostgresBinaryComparisonOperation(column_1,
                                                    PostgresConstant.createIntConstant(40000),
                                                    PostgresBinaryComparisonOperator.GREATER),
                                            BinaryLogicalOperator.AND),
                                    new PostgresBinaryLogicalOperation(
                                            new PostgresBinaryComparisonOperation(column_2,
                                                    PostgresConstant.createIntConstant(-15000),
                                                    PostgresBinaryComparisonOperator.GREATER),
                                            new PostgresBinaryComparisonOperation(column_2,
                                                    PostgresConstant.createIntConstant(-5000),
                                                    PostgresBinaryComparisonOperator.LESS_EQUALS),
                                            BinaryLogicalOperator.AND),
                                    BinaryLogicalOperator.AND),
                            BinaryLogicalOperator.AND);
                    break;
                case 2:
                    // Create fixed Query (t0.c0 > 5000 AND t0.c0 < 10000) AND (t0.c1 < 50000 AND t0.c1 > 45000) AND
                    // (t0.c2 > -10000 AND t0.c2 <= -5000)
                    randomWhereCondition = new PostgresBinaryLogicalOperation(new PostgresBinaryLogicalOperation(
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(0),
                                    PostgresBinaryComparisonOperator.GREATER),
                            new PostgresBinaryComparisonOperation(column_0, PostgresConstant.createIntConstant(5000),
                                    PostgresBinaryComparisonOperator.LESS),
                            BinaryLogicalOperator.AND),

                            new PostgresBinaryLogicalOperation(
                                    new PostgresBinaryLogicalOperation(
                                            new PostgresBinaryComparisonOperation(column_1,
                                                    PostgresConstant.createIntConstant(50000),
                                                    PostgresBinaryComparisonOperator.LESS),
                                            new PostgresBinaryComparisonOperation(column_1,
                                                    PostgresConstant.createIntConstant(45000),
                                                    PostgresBinaryComparisonOperator.GREATER),
                                            BinaryLogicalOperator.AND),
                                    new PostgresBinaryLogicalOperation(
                                            new PostgresBinaryComparisonOperation(column_2,
                                                    PostgresConstant.createIntConstant(-10000),
                                                    PostgresBinaryComparisonOperator.GREATER),
                                            new PostgresBinaryComparisonOperation(column_2,
                                                    PostgresConstant.createIntConstant(-5000),
                                                    PostgresBinaryComparisonOperator.LESS_EQUALS),
                                            BinaryLogicalOperator.AND),
                                    BinaryLogicalOperator.AND),
                            BinaryLogicalOperator.AND);
                    break;
                default:
                    throw new AssertionError();
                }
            default:
                randomWhereCondition = getRandomWhereCondition(columns);
                break;
            }
            // System.err.println(PostgresVisitor.asString(randomWhereCondition));
            List<PostgresTable> tables = randomTables.getTables();

            List<PostgresJoin> joinStatements = getJoinStatements(state, columns, tables);
            List<PostgresExpression> fromTables = tables.stream()
                    .map(t -> new PostgresFromTable(t, Randomly.getBoolean())).collect(Collectors.toList());
            secondCount = getUnoptimizedQueryCount(fromTables, randomWhereCondition, joinStatements);

            int firstCount = getOptimizedQueryCount(fromTables, columns, randomWhereCondition, joinStatements);
            if (firstCount == -1 || secondCount == -1) {
                throw new IgnoreMeException();
            }
            if (firstCount != secondCount) {
                String queryFormatString = "-- %s;\n-- count: %d";
                String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, firstCount);
                String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString,
                        secondCount);
                state.getState().getLocalState()
                        .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
                String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", firstCount,
                        secondCount, firstQueryStringWithCount, secondQueryStringWithCount);
                throw new AssertionError(assertionMessage);
            }
        } finally {
            csvWriter.append("," + String.valueOf(globalState.getDmbsSpecificOptions().fixedQueryNr) + ","
                    + String.valueOf(secondCount) + "," + String.valueOf(globalState.getDmbsSpecificOptions().queryDiff)
                    + "," + String.valueOf(globalState.getDmbsSpecificOptions().queryNr) + "\n");
            csvWriter.flush();
            csvWriter.close();
            if (secondCount > 0) {
                System.exit(1);
            }
        }
    }

    public static List<PostgresJoin> getJoinStatements(PostgresGlobalState globalState, List<PostgresColumn> columns,
            List<PostgresTable> tables) {
        List<PostgresJoin> joinStatements = new ArrayList<>();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < 0; i++) {
            PostgresExpression joinClause = gen.generateExpression(PostgresDataType.BOOLEAN);
            PostgresTable table = Randomly.fromList(tables);
            tables.remove(table);
            PostgresJoinType options = PostgresJoinType.getRandom();
            PostgresJoin j = new PostgresJoin(new PostgresFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < 0; i++) {
            PostgresTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            PostgresSubquery subquery = PostgresTLPBase.createSubquery(globalState, String.format("sub%d", i),
                    subqueryTables);
            PostgresExpression joinClause = gen.generateExpression(PostgresDataType.BOOLEAN);
            PostgresJoinType options = PostgresJoinType.getRandom();
            PostgresJoin j = new PostgresJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    private PostgresExpression getRandomWhereCondition(List<PostgresColumn> columns) {
        return new PostgresExpressionGenerator(state).setColumns(columns).generateExpression(PostgresDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<PostgresExpression> fromTables, PostgresExpression randomWhereCondition,
            List<PostgresJoin> joinStatements) throws SQLException {
        PostgresSelect select = new PostgresSelect();
        PostgresCastOperation isTrue = new PostgresCastOperation(randomWhereCondition,
                PostgresCompoundDataType.create(PostgresDataType.INT));
        PostgresPostfixText asText = new PostgresPostfixText(isTrue, " as count", null, PostgresDataType.INT);
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(fromTables);
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + PostgresVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        errors.add("canceling statement due to statement timeout");
        Query q = new QueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(List<PostgresExpression> randomTables, List<PostgresColumn> columns,
            PostgresExpression randomWhereCondition, List<PostgresJoin> joinStatements) throws SQLException {
        PostgresSelect select = new PostgresSelect();
        PostgresColumnValue allColumns = new PostgresColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(Arrays.asList(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new PostgresExpressionGenerator(state).setColumns(columns).generateOrderBy());
        }
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = PostgresVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
