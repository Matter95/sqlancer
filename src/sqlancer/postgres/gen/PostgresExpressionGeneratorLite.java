package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresCompoundDataType;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresRowValue;
import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresAggregate.PostgresAggregateFunction;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresCollate;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresOrderByTerm;
import sqlancer.postgres.ast.PostgresOrderByTerm.PostgresOrder;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixOperation.PostfixOperator;
import sqlancer.postgres.ast.PostgresPrefixOperation;

public class PostgresExpressionGeneratorLite extends PostgresExpressionGenerator{

    private final int maxDepth;

    private final Randomly r;

    private PostgresRowValue rw;
    
    private PostgresGlobalState globalState;

    public PostgresExpressionGeneratorLite(PostgresGlobalState globalState) {
    	super(globalState);
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
    }

    public PostgresExpressionGeneratorLite setRowValue(PostgresRowValue rw) {
        this.rw = rw;
        return this;
    }

    public PostgresExpression generateExpression(int depth) {
        return generateExpression(depth, Randomly.fromOptions(PostgresDataType.INT));
    }

    public List<PostgresExpression> generateOrderBy() {
        List<PostgresExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new PostgresOrderByTerm(PostgresColumnValue.create(Randomly.fromList(columns), null),
                    PostgresOrder.getRandomOrder()));
        }
        return orderBys;
    }
    /* 
     * ALL AVAILABLE OPTIONS:
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, LIKE, BETWEEN, IN_OPERATION,
        SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON;
    */
    private enum BooleanExpression {
        BINARY_COMPARISON;
    }

    private PostgresExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));

        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case BINARY_COMPARISON:
            PostgresDataType dataType = getMeaningfulType();
            return generateComparison(depth, dataType);
        default:
            throw new AssertionError();
        }
    }
    private PostgresDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return Randomly.fromOptions(PostgresDataType.INT, PostgresDataType.BOOLEAN);
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private PostgresExpression generateComparison(int depth, PostgresDataType dataType) {
        PostgresExpression leftExpr = generateConstant(new Randomly(), PostgresDataType.TEXT, true);
        PostgresExpression rightExpr = generateExpression(depth + 1, dataType);
        
        return getComparison(leftExpr, rightExpr);
    }
    
    /*
     * check expression
     */
    private PostgresExpression generateComparison(int depth, String columnName) {
        PostgresExpression leftExpr = generateExpression(PostgresDataType.INT);
        PostgresExpression rightExpr = generateExpression(depth + 1, PostgresDataType.INT);
        return getComparison(leftExpr, rightExpr);
    }

    private PostgresExpression getComparison(PostgresExpression leftExpr, PostgresExpression rightExpr) {
        List<PostgresBinaryComparisonOperator> validOptions = new ArrayList<>(Arrays.asList(PostgresBinaryComparisonOperator.values()));
        validOptions.remove(PostgresBinaryComparisonOperator.IS_DISTINCT);
        validOptions.remove(PostgresBinaryComparisonOperator.IS_NOT_DISTINCT);
        PostgresBinaryComparisonOperation op = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
                Randomly.fromList(validOptions));
        if (PostgresProvider.generateOnlyKnown && op.getLeft().getExpressionType() == PostgresDataType.TEXT
                && op.getRight().getExpressionType() == PostgresDataType.TEXT) {
            return new PostgresCollate(op, "C");
        }
        return op;
    }

    public PostgresExpression generateExpression(int depth, PostgresDataType originalType) {
        PostgresDataType dataType = originalType;
        if (dataType == PostgresDataType.REAL) {
            dataType = PostgresDataType.INT;
        }
        if (dataType == PostgresDataType.FLOAT) {
            dataType = PostgresDataType.INT;
        }
        PostgresExpression exprInternal = generateExpressionInternal(depth, dataType);
        return exprInternal;
    }


    private PostgresExpression generateExpressionInternal(int depth, PostgresDataType dataType) throws AssertionError {

        if (depth > maxDepth || Randomly.getBoolean()) {
            // generic expression
        	if (filterColumns(dataType).isEmpty()) {
                return generateConstant(r, dataType, false);
            } else {
                return createColumnOfType(dataType);
            }
        } else {
            switch (dataType) {           
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case INT:
            case DECIMAL:
            case REAL:
            case FLOAT:
            case MONEY:
            case INET:
                return generateConstant(r, PostgresDataType.INT, false);
            default:
                throw new AssertionError(dataType);
            }
        }
    }

    private static PostgresCompoundDataType getCompoundDataType(PostgresDataType type) {
        switch (type) {
        case BOOLEAN:
        case DECIMAL: // TODO
        case FLOAT:
        case INT:
        case MONEY:
        case RANGE:
        case REAL:
        case INET:
            return PostgresCompoundDataType.create(type);
        case TEXT: // TODO
        case BIT:
            if (Randomly.getBoolean() || PostgresProvider.generateOnlyKnown /*
                                                                             * The PQS implementation does not check for
                                                                             * size specifications
                                                                             */) {
                return PostgresCompoundDataType.create(type);
            } else {
                return PostgresCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
            }
        default:
            throw new AssertionError(type);
        }

    }

    protected PostgresExpression createColumnOfType(PostgresDataType type) {
        List<PostgresColumn> columns = filterColumns(type);
        PostgresColumn fromList = Randomly.fromList(columns);
        PostgresConstant value = rw == null ? null : rw.getValues().get(fromList);
        return PostgresColumnValue.create(fromList, value);
    }

    public PostgresExpression generateExpressionWithExpectedResult(PostgresDataType type) {
        this.expectedResult = true;
        PostgresExpressionGeneratorLite gen = (PostgresExpressionGeneratorLite) new PostgresExpressionGeneratorLite(globalState).setColumns(columns)
                .setRowValue(rw);
        PostgresExpression expr;
        do {
            expr = gen.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    public static PostgresExpression generateConstant(Randomly r, PostgresDataType type, boolean textVar) {

        // if (Randomly.getBooleanWithSmallProbability()) {
        // return PostgresConstant.createTextConstant(r.getString());
        // }
        switch (type) {
        case INT:
            if (textVar) {
                return PostgresConstant.createTextConstant(String.valueOf(r.getInteger()));
            } else {
                return PostgresConstant.createIntConstant(r.getInteger());
            }
        case BOOLEAN:
            if (Randomly.getBooleanWithSmallProbability() && !PostgresProvider.generateOnlyKnown) {
                return PostgresConstant
                        .createTextConstant(Randomly.fromOptions("TR", "TRUE", "FA", "FALSE", "0", "1", "ON", "off"));
            } else {
                return PostgresConstant.createBooleanConstant(Randomly.getBoolean());
            }
        case TEXT:
            return PostgresConstant.createTextConstant(r.getString());
        case DECIMAL:
            return PostgresConstant.createDecimalConstant(r.getRandomBigDecimal());
        case FLOAT:
            return PostgresConstant.createFloatConstant((float) r.getDouble());
        case REAL:
            return PostgresConstant.createDoubleConstant(r.getDouble());
        case RANGE:
            return PostgresConstant.createRange(r.getInteger(), Randomly.getBoolean(), r.getInteger(),
                    Randomly.getBoolean());
        case MONEY:
            return new PostgresCastOperation(generateConstant(r, PostgresDataType.FLOAT, false),
                    getCompoundDataType(PostgresDataType.MONEY));
        case INET:
            return PostgresConstant.createInetConstant(getRandomInet(r));
        case BIT:
            return PostgresConstant.createBitConstant(r.getInteger());
        default:
            throw new AssertionError(type);
        }
    }

    private static String getRandomInet(Randomly r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i != 0) {
                sb.append('.');
            }
            sb.append(r.getInteger() & 255);
        }
        return sb.toString();
    }

    public static PostgresExpression generateExpression(PostgresGlobalState globalState, List<PostgresColumn> columns,
            PostgresDataType type) {
        return new PostgresExpressionGeneratorLite(globalState).setColumns(columns).generateExpression(0, type);
    }
    
    public static PostgresExpression generateCheckExpression(PostgresGlobalState globalState, List<PostgresColumn> columns,
            String columName) {
        return ((PostgresExpressionGeneratorLite) new PostgresExpressionGeneratorLite(globalState).setColumns(columns)).generateComparison(0, columName);
    }


    public List<PostgresExpression> generateExpressions(int nr) {
        List<PostgresExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(0));
        }
        return expressions;
    }

    
    
    public PostgresExpression generateExpression(PostgresDataType dataType) {
        return generateExpression(0, dataType);
    }

    public PostgresExpression generateCheckExpression(PostgresDataType dataType) {
        return generateExpression(0, dataType);
    }
    
    public PostgresExpressionGeneratorLite setGlobalState(PostgresGlobalState globalState) {
        this.globalState = globalState;
        return this;
    }

    public PostgresExpression generateHavingClause() {
        this.allowAggregateFunctions = true;
        PostgresExpression expression = generateExpression(PostgresDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

    public PostgresExpression generateAggregate() {
        return getAggregate(PostgresDataType.getRandomType());
    }

    protected PostgresExpression getAggregate(PostgresDataType dataType) {
        List<PostgresAggregateFunction> aggregates = PostgresAggregateFunction.getAggregates(dataType);
        PostgresAggregateFunction agg = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, agg);
    }

    public PostgresAggregate generateArgsForAggregate(PostgresDataType dataType, PostgresAggregateFunction agg) {
        List<PostgresDataType> types = agg.getTypes(dataType);
        List<PostgresExpression> args = new ArrayList<>();
        for (PostgresDataType argType : types) {
            args.add(generateExpression(argType));
        }
        return new PostgresAggregate(args, agg);
    }

    public PostgresExpressionGeneratorLite allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    @Override
    public PostgresExpression generatePredicate() {
        return generateExpression(PostgresDataType.BOOLEAN);
    }

    @Override
    public PostgresExpression negatePredicate(PostgresExpression predicate) {
        return new PostgresPrefixOperation(predicate, PostgresPrefixOperation.PrefixOperator.NOT);
    }

    @Override
    public PostgresExpression isNull(PostgresExpression expr) {
        return new PostgresPostfixOperation(expr, PostfixOperator.IS_NULL);
    }

}
