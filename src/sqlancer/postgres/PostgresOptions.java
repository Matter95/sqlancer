package sqlancer.postgres;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.postgres.PostgresOptions.PostgresOracleFactory;
import sqlancer.postgres.oracle.PostgresNoRECOracle;
import sqlancer.postgres.oracle.PostgresNoRECOracleLite;
import sqlancer.postgres.oracle.PostgresPivotedQuerySynthesisOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle;
import sqlancer.postgres.oracle.tlp.PostgresTLPWhereOracle;

@Parameters
public class PostgresOptions implements DBMSSpecificOptions<PostgresOracleFactory> {

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<PostgresOracleFactory> oracle = Arrays.asList(PostgresOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = "postgresql://localhost:5432/test";

    @Parameter(names = "--use-simple-expression-generator", description = "Specifies whether to use the lite expression generator or the full generator", arity = 1)
    public boolean useSimpleExpressionGenerator = false;

    @Parameter(names = "--use-modified-oracle", description = "Specifies whether to use the lite expression generator or the full generator", arity = 1)
    public boolean useModifiedOracle = false;

    @Parameter(names = "--activate-db-checks", description = "Specifies whether to use database check statements to fill the database with values", arity = 1)
    public boolean activateDbChecks = false;

    @Parameter(names = "--standard-run", description = "Specifies whether to fill the database with values or not", arity = 1)
    public boolean standardRun = true;

    @Parameter(names = "--fixed-query", description = "Specifies whether to use a fixed query or not")
    public boolean fixedQuery = false;

    @Parameter(names = "--number-of-checks", description = "Specifies how many check statements per column are maximally generated")
    public int nrChecks = 3;

    @Parameter(names = "--number-of-values", description = "Specifies how many values per insert are added")
    public int nrValues = 5;

    @Parameter(names = "--number-of-tables", description = "Specifies how many tables a database should generate")
    public int nrTables = 4;

    @Parameter(names = "--number-of-columns", description = "Specifies how many columns a table should generate")
    public int nrColumns = 4;

    @Parameter(names = "--number-of-inserts", description = "Specifies how many times the insert action is called")
    public int nrInserts = 10;

    @Parameter(names = "--output-path", description = "Specifies in which file to save the elapsed time logs")
    public String path = "times.csv";

    @Parameter(names = "--fixed-query-number", description = "Specifies which fixed query to choose")
    public int fixedQueryNr = 0;

    public enum PostgresOracleFactory implements OracleFactory<PostgresGlobalState> {
        NOREC {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                if (globalState.getDmbsSpecificOptions().useModifiedOracle) {
                    return new PostgresNoRECOracleLite(globalState);
                } else {
                    return new PostgresNoRECOracle(globalState);
                }
            }
        },
        PQS {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        HAVING {

            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                return new PostgresTLPHavingOracle(globalState);
            }

        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(PostgresGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new PostgresTLPWhereOracle(globalState));
                oracles.add(new PostgresTLPHavingOracle(globalState));
                oracles.add(new PostgresTLPAggregateOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

    }

    @Override
    public List<PostgresOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
