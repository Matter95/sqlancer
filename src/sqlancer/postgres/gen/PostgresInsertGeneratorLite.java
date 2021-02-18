package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.PostgresGlobalState.Tuple;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;

import com.microsoft.z3.*;



	public final class PostgresInsertGeneratorLite {
		public PostgresInsertGeneratorLite() {
    }
	
    public static Query insert(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("cannot insert into column");
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonInsertUpdateErrors(errors);
        PostgresCommon.addCommonExpressionErrors(errors);
        errors.add("multiple assignments to same column");
        errors.add("violates foreign key constraint");
        errors.add("value too long for type character varying");
        errors.add("conflicting key value violates exclusion constraint");
        errors.add("violates not-null constraint");
        errors.add("current transaction is aborted");
        errors.add("bit string too long");
        errors.add("new row violates check option for view");
        errors.add("reached maximum value of sequence");
        errors.add("but expression is of type");
        StringBuilder sb = new StringBuilder();
        
        List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();

        //get random ceiling for number generation
		int n = Randomly.smallNumber() + 1;   	    		
    	
        //initialize used Numbers
    	globalState.initializeUsedNumbersSat(globalState.getSchema().getDatabaseTables().size());
    	globalState.initializeUsedNumbersNsat(globalState.getSchema().getDatabaseTables().size());
    	//generate a Z3 solver for satisfying the constraint and one that does not and initialize it accordingly
    	Context ctxt = new Context();
    	Solver s = ctxt.mkSolver(); 
    	
    	List<List<String>> colNums = new ArrayList<>();
    	for(PostgresColumn column : columns) {
	    	colNums.add(insertRow(s, ctxt, globalState, column, table, false, n));
    	}
    	
    	for(int i = 0; i < n; i++){
    		sb.append("INSERT INTO ");
            sb.append(table.getName());
            sb.append("(");
            sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
                    
            sb.append(" VALUES");
            sb.append("(");
            //TODO: Find OUTOFBOUNDS ERROR CAUSE
            //add the gathered values into the subset
            for(int j = 0; j < colNums.size(); j++) {
            	sb.append(colNums.get(j).get(i));
            	if(j < colNums.size() - 1) {
            		sb.append(", ");
            	}
            }
            sb.append(")");
            if(i < n - 1) {
        		sb.append(";");
                sb.append("\n");
        	}
    	}    
        
        errors.add("duplicate key value violates unique constraint");
        errors.add("identity column defined as GENERATED ALWAYS");
        errors.add("out of range");
        errors.add("violates check constraint");
        errors.add("no partition of relation");
        errors.add("invalid input syntax");
        errors.add("division by zero");
        errors.add("violates foreign key constraint");
        errors.add("data type unknown");
        return new QueryAdapter(sb.toString(), errors);
    }

    public static Query insert(PostgresGlobalState globalState, Solver s, Context ctxt) {
        List<PostgresTable> tables = globalState.getSchema().getTables(t -> t.isInsertable());
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("cannot insert into column");
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonInsertUpdateErrors(errors);
        PostgresCommon.addCommonExpressionErrors(errors);
        errors.add("multiple assignments to same column");
        errors.add("violates foreign key constraint");
        errors.add("value too long for type character varying");
        errors.add("conflicting key value violates exclusion constraint");
        errors.add("violates not-null constraint");
        errors.add("current transaction is aborted");
        errors.add("bit string too long");
        errors.add("new row violates check option for view");
        errors.add("reached maximum value of sequence");
        errors.add("but expression is of type");
        StringBuilder sb = new StringBuilder();
        //initialize used Numbers
    	globalState.initializeUsedNumbersSat(globalState.getSchema().getDatabaseTables().size());
    	globalState.initializeUsedNumbersNsat(globalState.getSchema().getDatabaseTables().size());
    	int tSize = tables.size();
    	
		for(PostgresTable table : tables) {
	        List<PostgresColumn> columns = table.getColumns();

	    	//generate a Z3 solver for satisfying the constraint and one that does not and initialize it accordingly
	    	int size = columns.size();
	    	for(PostgresColumn column : columns) {
		    	sb.append("INSERT INTO ");
		        sb.append(table.getName());
		    	sb.append("(");
		        sb.append(column.getName());
		        sb.append(")");
		                
		        sb.append(" VALUES");
		        
		        insertRow(s, ctxt, globalState,sb, column, table, false);
		        if(size > 1)
		        	sb.append("\n");
		        size--;
	    	}
	        if(tSize > 1)
	        	sb.append("\n");
	        tSize--;
        }
        errors.add("duplicate key value violates unique constraint");
        errors.add("identity column defined as GENERATED ALWAYS");
        errors.add("out of range");
        errors.add("violates check constraint");
        errors.add("no partition of relation");
        errors.add("invalid input syntax");
        errors.add("division by zero");
        errors.add("violates foreign key constraint");
        errors.add("data type unknown");
        
        return new QueryAdapter(sb.toString(), errors);
    }
    
    
	private static void insertRow(Solver s, Context ctxt, PostgresGlobalState globalState, StringBuilder sb, PostgresColumn column,
            PostgresTable table, boolean sat) throws IgnoreMeException {
        s.reset();

    	int tableNr = getTableNumber(table.getName());
    	int columnNr = getTableNumber(column.getName());

        //get the list of checks for the given table
    	ArrayList<Tuple> currConstraints;
    	//Add all used Numbers to the constraints
    	if(sat) {
    		currConstraints = globalState.getUsedNumbersSat(tableNr);
    	}
    	else {
    		currConstraints = globalState.getUsedNumbersNsat(tableNr);	
    	}
    	
    	String currColumn = table.getName() + "." + column.getName();
    	//System.err.println("Loop iteration: " + i + " | currCol: " + currColumn);
    	ArrayList<PostgresExpression> checks = globalState.getCheckStatementsOfTableNColumnM(tableNr, columnNr);
    	
    	
    	//for each Tuple of the given table
    	for (Tuple t : currConstraints) {
    		if(t.varNameIsEqual(currColumn)) {
    			//add not equal constraint for the already used value
		     	s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(currColumn), ctxt.mkInt(t.getVal()))));
    		}
		}
    	String var = "";
    	
    	boolean varToEvaluate = false;
    	boolean eqNeq = false;
		for(PostgresExpression expr : checks) {
			boolean rightIsVar = false;
			boolean leftIsVar = false;
			PostgresExpression left;
			PostgresExpression right;
			
			//System.err.println(PostgresVisitor.asString(expr));
			
			if (expr instanceof BinaryNode) {
				@SuppressWarnings("unchecked")
				BinaryNode<PostgresExpression> comp = (BinaryNode<PostgresExpression>) expr;
				left = comp.getLeft();	
				right = comp.getRight();
			} else {
				//Expression is not a comparison (!Binary Node)
				throw new IgnoreMeException();
			}
			String l = varToString(left);
			String r = varToString(right);

			//variable we will evaluate on
			if(!l.equals("noVar")) {
				var = l;
				leftIsVar = true;
			}
			if(!r.equals("noVar")) {
				var = r;
				rightIsVar = true;
			}	
			//create argument when a variable is present
			if((leftIsVar || rightIsVar) && (!leftIsVar || !rightIsVar)) {
				ArithExpr arg0; 
				ArithExpr arg1; 

				if(rightIsVar) 
					arg1 = ctxt.mkIntConst(PostgresVisitor.asString(right));
				else
					arg1 = ctxt.mkInt(Integer.parseInt(right.toString()));
				if(leftIsVar)
					arg0 = ctxt.mkIntConst(PostgresVisitor.asString(left));
				else
					arg0 = ctxt.mkInt(Integer.parseInt(left.toString()));
				//System.err.println("Argument 0: " + arg0 + " | Argument 1: " + arg1);
				BoolExpr e = makeBoolExpr(ctxt, arg0, arg1, getOperator(expr));
				
				if(!sat) {				
					e = ctxt.mkNot(e);
				}
				s.add(e);
        		varToEvaluate = true;
			}
			//single constraint edge case
			if(checks.size() == 1 && varToEvaluate) {
    			//Special Case EQ/NEQ
				boolean isEqSat = getOperator(expr) == Ops.EQUAL && sat;
				boolean isNeqNsat = getOperator(expr) == Ops.NOT_EQUAL && !sat;
				boolean isSC = isEqSat || isNeqNsat;
				if(isSC) {
	            	sb.append("(");    	
					if(isEqSat) {
						if(rightIsVar) {
    						sb.append(PostgresVisitor.asString(left));
						} else if(leftIsVar) {
    						sb.append(PostgresVisitor.asString(right));
						}
						eqNeq = true;
    				} else if(isNeqNsat) {
    					if(rightIsVar) {
    						sb.append(PostgresVisitor.asString(left));
						} else if(leftIsVar) {
    						sb.append(PostgresVisitor.asString(right));
						}
						eqNeq = true;		    					
					}
	            	sb.append(")");    	
				}
			}
		}
		Randomly rand = new Randomly();
		//choose how many values are inserted per column
		int n = rand.getInteger(1, globalState.getDmbsSpecificOptions().nrInsertValues);   	    		
		if(!varToEvaluate) {
            for (int j = 0; j < n; j++) {
            	sb.append("(");    	
				//just use random numbers
        		PostgresExpression generateConstant;
                generateConstant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                        column.getType());
	            sb.append(PostgresVisitor.asString(generateConstant));
            	sb.append(")");
            	if(j < n-1)
	            	sb.append(",");    	

            }
		} else if(eqNeq) {
			//if special case equal or not(not equal) [Not Satisfying] occured skip model evaluation
		} else {
            for (int j = 0; j < n; j++) {
				Status state = s.check();
				//check if statement is satisfiable
	        	if(state.toInt() > 0) {
	            	sb.append("(");    	

	                //evaluate current column
    				Expr e = s.getModel().eval(ctxt.mkIntConst(currColumn), true);
    				int x = modelToInt(e);
    				s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(currColumn), ctxt.mkInt(x))));
    				
    				//System.err.println("name | value: " + name + " | " + ctxt.mkInt(x));
    				PostgresExpression generateConstantFromZ3 = PostgresConstant.createIntConstant(x);
    				if(sat)
    					globalState.addUsedNumberSat(tableNr, var, x);
    				else
    					globalState.addUsedNumberNsat(tableNr, var, x);

    				sb.append(PostgresVisitor.asString(generateConstantFromZ3));
	            	sb.append(")");    	
	            	if(j < n-1)
		            	sb.append(",");  
				//not satisfiable
	        	} else 
	        		throw new IgnoreMeException();
            } 
		}
		sb.append(";");
    }
    
	private static List<String> insertRow(Solver s, Context ctxt, PostgresGlobalState globalState, PostgresColumn column,
            PostgresTable table, boolean sat, int n) throws IgnoreMeException {
        s.reset();

        List<String> numbers = new ArrayList<>();
    	int tableNr = getTableNumber(table.getName());
    	int columnNr = getTableNumber(column.getName());
    	int ind = 0;

        //get the list of checks for the given table
    	ArrayList<Tuple> currConstraints;
    	//Add all used Numbers to the constraints
    	if(sat) {
    		currConstraints = globalState.getUsedNumbersSat(tableNr);
    	}
    	else {
    		currConstraints = globalState.getUsedNumbersNsat(tableNr);	
    	}
    	
    	String currColumn = table.getName() + "." + column.getName();
    	//System.err.println("Loop iteration: " + i + " | currCol: " + currColumn);
    	ArrayList<PostgresExpression> checks = globalState.getCheckStatementsOfTableNColumnM(tableNr, columnNr);
    	
    	
    	//for each Tuple of the given table
    	for (Tuple t : currConstraints) {
    		if(t.varNameIsEqual(currColumn)) {
    			//add not equal constraint for the already used value
		     	s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(currColumn), ctxt.mkInt(t.getVal()))));
    		}
		}
    	String var = "";
    	
    	boolean varToEvaluate = false;
    	boolean eqNeq = false;
		for(PostgresExpression expr : checks) {
			boolean rightIsVar = false;
			boolean leftIsVar = false;
			PostgresExpression left;
			PostgresExpression right;
			
			//System.err.println(PostgresVisitor.asString(expr));
			
			if (expr instanceof BinaryNode) {
				@SuppressWarnings("unchecked")
				BinaryNode<PostgresExpression> comp = (BinaryNode<PostgresExpression>) expr;
				left = comp.getLeft();	
				right = comp.getRight();
			} else {
				//Expression is not a comparison (!Binary Node)
				throw new IgnoreMeException();
			}
			String l = varToString(left);
			String r = varToString(right);

			//variable we will evaluate on
			if(!l.equals("noVar")) {
				var = l;
				leftIsVar = true;
			}
			if(!r.equals("noVar")) {
				var = r;
				rightIsVar = true;
			}	
			//create argument when a variable is present
			if((leftIsVar || rightIsVar) && (!leftIsVar || !rightIsVar)) {
				ArithExpr arg0; 
				ArithExpr arg1; 

				//TODO: some numbers seem to exceed Integer range
				if(rightIsVar) 
					arg1 = ctxt.mkIntConst(PostgresVisitor.asString(right));
				else
					arg1 = ctxt.mkInt(Integer.parseInt(right.toString()));
				if(leftIsVar)
					arg0 = ctxt.mkIntConst(PostgresVisitor.asString(left));
				else
					arg0 = ctxt.mkInt(Integer.parseInt(left.toString()));
				//System.err.println("Argument 0: " + arg0 + " | Argument 1: " + arg1);
				BoolExpr e = makeBoolExpr(ctxt, arg0, arg1, getOperator(expr));
				
				if(!sat) {				
					e = ctxt.mkNot(e);
				}
				s.add(e);
        		varToEvaluate = true;
			}
			//single constraint edge case
			if(checks.size() == 1 && varToEvaluate) {
    			//Special Case EQ/NEQ
				boolean isEqSat = getOperator(expr) == Ops.EQUAL && sat;
				boolean isNeqNsat = getOperator(expr) == Ops.NOT_EQUAL && !sat;
				boolean isSC = isEqSat || isNeqNsat;
				if(isSC) {
					String num = "";
	            	if(isEqSat) {
						if(rightIsVar) {
    						num = PostgresVisitor.asString(left);
						} else if(leftIsVar) {
							num = PostgresVisitor.asString(right);						}
						eqNeq = true;
    				} else if(isNeqNsat) {
    					if(rightIsVar) {
    						num = PostgresVisitor.asString(left);
						} else if(leftIsVar) {
							num = PostgresVisitor.asString(right);
						}
						eqNeq = true;		    					
					} 
	            	for (int j = 0; j < n; j++) {
	            		numbers.add(num);
	            	}
				}
			}
			ind++;
		}
		Randomly rand = new Randomly();
		//choose how many values are inserted per column
		if(!varToEvaluate) {
            for (int j = 0; j < n; j++) {
            	//just use random numbers
        		PostgresExpression generateConstant;
                generateConstant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                        column.getType());
                numbers.add(PostgresVisitor.asString(generateConstant));
            }
		} else if(eqNeq) {
			//if special case equal or not(not equal) [Not Satisfying] occured skip model evaluation
		} else {
            for (int j = 0; j < n; j++) {
				Status state = s.check();
				//check if statement is satisfiable
	        	if(state.toInt() > 0) {

	                //evaluate current column
    				Expr e = s.getModel().eval(ctxt.mkIntConst(currColumn), true);
    				int x = modelToInt(e);
    				s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(currColumn), ctxt.mkInt(x))));
    				
    				//System.err.println("name | value: " + name + " | " + ctxt.mkInt(x));
    				PostgresExpression generateConstantFromZ3 = PostgresConstant.createIntConstant(x);
    				if(sat)
    					globalState.addUsedNumberSat(tableNr, var, x);
    				else
    					globalState.addUsedNumberNsat(tableNr, var, x);

    				numbers.add(PostgresVisitor.asString(generateConstantFromZ3));
				//not satisfiable
	        	} else 
	        		throw new IgnoreMeException();
            } 
		}
		
		return numbers;
    }
    
    
    public static boolean isVar(PostgresExpression expr) {
    	String var = PostgresVisitor.asString(expr);
    	return var.contains("t");
    }

    
    public static BoolExpr makeBoolExpr(Context ctxt, ArithExpr arg0, ArithExpr arg1, Ops op) {
    	switch (op) {
		case GREATER_EQUAL:
			return ctxt.mkGe(arg0, arg1);				
		case GREATER_THAN:
			return ctxt.mkGt(arg0, arg1);			
		case LESS_EQUAL:
			return ctxt.mkLe(arg0, arg1);
		case LESS_THAN:
			return ctxt.mkLt(arg0, arg1);				
		case EQUAL:
			return ctxt.mkEq(arg0, arg1);				
		case NOT_EQUAL:
			return ctxt.mkNot(ctxt.mkEq(arg0, arg1));
		default:
			throw new IgnoreMeException();
		}
    }
    
    public enum Ops {
    	GREATER_EQUAL, GREATER_THAN, LESS_EQUAL, LESS_THAN, EQUAL, NOT_EQUAL, NOP;
    }
    
    public static Ops getOperator(PostgresExpression expr) {
    	String check = PostgresVisitor.asString(expr);
    	String ge = ">=";
    	String gt = ">";
    	String le = "<=";
    	String lt = "<";
    	String eq = "=";
    	String neq = "!=";

    	
    	if(check.contains(ge)) {
    		return Ops.GREATER_EQUAL;
    	}
    	else if(check.contains(gt)) {
    		return Ops.GREATER_THAN;
    	}
    	else if(check.contains(le)) {
    		return Ops.LESS_EQUAL;
    	}
    	else if(check.contains(lt)) {
    		return Ops.LESS_THAN;
    	}
    	else if(check.contains(neq)) {
    		return Ops.NOT_EQUAL;
    	}
    	else if(check.contains(eq)) {
    		return Ops.EQUAL;
    	}
    	else {
    		return Ops.NOP;
    	}
    }
    
    private static int getTableNumber(String tableName) {
    	return Integer.parseInt(tableName.substring(1));
    }
	
    private static int modelToInt(Expr e) throws IgnoreMeException{
    	if(e.isInt()) {
			IntNum eIntSat = (IntNum) e;
			return eIntSat.getInt();
		} else {
			throw new IgnoreMeException();
		}
    }
    
    public static String varToString(PostgresExpression var) {
    	if(isVar(var))
    		return PostgresVisitor.asString(var);
    	else
    		return "noVar";
    }
}
