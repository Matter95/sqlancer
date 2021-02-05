package sqlancer.postgres;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import sqlancer.GlobalState;
import sqlancer.Randomly;
import sqlancer.postgres.ast.PostgresExpression;

public class PostgresGlobalState extends GlobalState<PostgresOptions, PostgresSchema> {

	public class Tuple {
		private String var;
		private int val;
		
		public Tuple(String var, int val) {
			this.val = val;
			this.var = var;
		}
		
		public String getVar() {
			return var;
		}
		public int getVal() {
			return val;
		}
		
		public boolean varNameIsEqual(String search) {
			if(var.equals(search)) {
				return true;
			} else {
				return false;
			}
		}
	}
	
    public static final char IMMUTABLE = 'i';
    public static final char STABLE = 's';
    public static final char VOLATILE = 'v';

    private List<String> operators = Collections.emptyList();
    private List<String> collates = Collections.emptyList();
    private List<String> opClasses = Collections.emptyList();
    // store check statements and give access to them
	private ArrayList<ArrayList<ArrayList<PostgresExpression>>> checkStatements = new ArrayList<ArrayList<ArrayList<PostgresExpression>>>();
	//store already inserted values
	private static ArrayList<ArrayList<Tuple>> usedNumbersSat = new ArrayList<ArrayList<Tuple>>();
	private static ArrayList<ArrayList<Tuple>> usedNumbersNsat = new ArrayList<ArrayList<Tuple>>();

    // store and allow filtering by function volatility classifications
    private final Map<String, Character> functionsAndTypes = new HashMap<>();
    private List<Character> allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);

    public PostgresGlobalState() {
    	//TODO: maybe make these values options?
    	initializeCheckStatements(8,10);
    }
    
    @Override
    public void setConnection(Connection con) {
        super.setConnection(con);
        try {
            this.opClasses = getOpclasses(getConnection());
            this.operators = getOperators(getConnection());
            this.collates = getCollnames(getConnection());
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private List<String> getCollnames(Connection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT collname FROM pg_collation WHERE collname LIKE '%utf8' or collname = 'C';")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

    private List<String> getOpclasses(Connection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select opcname FROM pg_opclass;")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

    private List<String> getOperators(Connection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT oprname FROM pg_operator;")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

    public List<String> getOperators() {
        return operators;
    }

    public String getRandomOperator() {
        return Randomly.fromList(operators);
    }

    public List<String> getCollates() {
        return collates;
    }

    public String getRandomCollate() {
        return Randomly.fromList(collates);
    }

    public List<String> getOpClasses() {
        return opClasses;
    }

    public String getRandomOpclass() {
        return Randomly.fromList(opClasses);
    }

    @Override
    public PostgresSchema readSchema() throws SQLException {
        return PostgresSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public void addFunctionAndType(String functionName, Character functionType) {
        this.functionsAndTypes.put(functionName, functionType);
    }

    public Map<String, Character> getFunctionsAndTypes() {
        return this.functionsAndTypes;
    }

    public void setAllowedFunctionTypes(List<Character> types) {
        this.allowedFunctionTypes = types;
    }

    public void setDefaultAllowedFunctionTypes() {
        this.allowedFunctionTypes = Arrays.asList(IMMUTABLE, STABLE, VOLATILE);
    }

    public List<Character> getAllowedFunctionTypes() {
        return this.allowedFunctionTypes;
    }
	public ArrayList<PostgresExpression> getCheckStatementsOfTableNColumnM(int i, int j) {
		return checkStatements.get(i).get(j);
	}
	public ArrayList<ArrayList<PostgresExpression>> getCheckStatementsOfTableN(int i) {
		return checkStatements.get(i);
	}
	public ArrayList<ArrayList<ArrayList<PostgresExpression>>> getCheckStatements() {
		return checkStatements;
	}
	public void initializeCheckStatements(int tables, int columns) {
		//initialize every table
		for(int i = 0; i < tables; i++) {
			checkStatements.add(new ArrayList<ArrayList<PostgresExpression>>());
			//initialize every column for each table
			for(int j = 0; j < columns; j++) {
				checkStatements.get(i).add(new ArrayList<PostgresExpression>());
			}

		}
	}

	public void addCheckStatementsForTableNColumnM(PostgresExpression item, int i, int j) {
		checkStatements.get(i).get(j).add(item);
	}
	
	public void clearCheckStatements(int n) {
		this.checkStatements.get(n).clear();
	}
	
	public void clearAllCheckStatements() {
		for(int i = 0; i < this.checkStatements.size(); i++) {
			this.checkStatements.get(i).clear();
		}
	}
	
	public void initializeUsedNumbersSat(int n) {
		for(int i = 0; i < n; i++) {
			usedNumbersSat.add(new ArrayList<Tuple>());
		}
	}

	public void addUsedNumberSat(int i, String var, int val) {
		boolean dup = false;
		ArrayList<Tuple> workset = usedNumbersSat.get(i);
		//check for duplicates
		for(Tuple t : workset) {
			if(t.var.equals(var) && t.val == val)
				dup = true;
		}
		if(!dup)
			workset.add(new Tuple(var, val));

	}
	//returns the used numbers of column n
	public ArrayList<Tuple> getUsedNumbersSat(int i) {
		return usedNumbersSat.get(i);
	}
	
	public void initializeUsedNumbersNsat(int n) {
		for(int i = 0; i < n; i++) {
			usedNumbersNsat.add(new ArrayList<Tuple>());
		}
	}
	
	public void addUsedNumberNsat(int i, String var, int val) {
		boolean dup = false;
		ArrayList<Tuple> workset = usedNumbersNsat.get(i);
		//check for duplicates
		for(Tuple t : workset) {
			if(t.var.equals(var) && t.val == val)
				dup = true;
		}
		if(!dup)
			workset.add(new Tuple(var, val));

	}
	//returns the used numbers of column n
	public ArrayList<Tuple> getUsedNumbersNsat(int i) {
		return usedNumbersNsat.get(i);
	}
}
