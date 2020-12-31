package sqlancer.postgres.gen;

import java.math.BigInteger;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.GlobalState;
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
import com.mysql.cj.exceptions.WrongArgumentException;



	public final class PostgresInsertGeneratorZ3 {
		PostgresInsertGeneratorZ3() {
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

        //initialize used Numbers
    	globalState.initializeUsedNumbersSat(globalState.getSchema().getDatabaseTables().size());
    	globalState.initializeUsedNumbersNsat(globalState.getSchema().getDatabaseTables().size());

    	//generate a Z3 solver for satisfying the constraint and one that does not and initialize it accordingly
    	Context ctxt = new Context();
    	Solver s = ctxt.mkSolver(); 
    	
        //one insert satisfying the constraints and one that does not
        String cols = columns.stream().map(c -> c.getName()).sorted().collect(Collectors.joining(", "));
        for(int j = 0; j < 2; j++) {
        	sb.append("INSERT INTO ");
            sb.append(table.getName());
        	sb.append("(");
            sb.append(cols);
            sb.append(")");
                    
            sb.append(" VALUES");
            
        	
            int n = Randomly.smallNumber() + 1;
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                s.reset();
                //TODO: make j == 0 random
                insertRow(s, ctxt, globalState,sb, columns, n == 1, table, j == 0);
            }
            if(j == 0) {
            	sb.append(";\n");
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

	private static void insertRow(Solver s, Context ctxt, PostgresGlobalState globalState, StringBuilder sb, List<PostgresColumn> columns,
            boolean canBeDefault, PostgresTable table, boolean sat) throws IgnoreMeException {


    	sb.append("(");    	
    	int tableNr = getTableNumber(table.getName());
        //get the list of checks for the given table
    	ArrayList<PostgresExpression> checks = globalState.getCheckStatementsOfTableN(tableNr);
    	ArrayList<Tuple> currConstraints;
    	//Add all used Numbers to the constraints
    	if(sat) {
    	currConstraints = globalState.getUsedNumbersSat(tableNr);
    	}
    	else {
    	currConstraints = globalState.getUsedNumbersNsat(tableNr);	
    	}
    	
    	
    	//go through each column of the table and insert values
        for (int i = 0; i < columns.size(); i++) {
        	String currColumn = table.getName() + "." + table.getColumns().get(i).getName();

        	if (i != 0) {
                sb.append(", ");
            }
        	//for each Tuple of the given table
        	for (Tuple t : currConstraints) {
        		if(t.varNameIsEqual(currColumn)) {
        			//add not equal constraint for the given value
    		     	s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(currColumn), ctxt.mkInt(t.getVal()))));
        		}
    		}
        	String var = "";
        	
            if (!Randomly.getBooleanWithSmallProbability() || !canBeDefault) {
        	
    			PostgresExpression expr = checks.get(i);
    			boolean rightIsVar = false;
				boolean leftIsVar = false;
				PostgresExpression left;
				PostgresExpression right;
				
    			if (expr instanceof BinaryNode) {
    				@SuppressWarnings("unchecked")
    				BinaryNode<PostgresExpression> comp = (BinaryNode<PostgresExpression>) expr;
    				left = comp.getLeft();	
    				right = comp.getRight();
    				if(isVar(left) && isVar(right)) {
    					leftIsVar = true;
    					rightIsVar = true;
    					var = "both";
    				} else if(isVar(left)) {
    					var = PostgresVisitor.asString(left);
    					leftIsVar = true;
    				} else if(isVar(right)) {
    					var = PostgresVisitor.asString(right);	
    					rightIsVar = true;
    				}
    				
    				//create argument when a variable is present
					if(rightIsVar || leftIsVar) {
						ArithExpr arg0; 
						ArithExpr arg1; 

						if(rightIsVar) 
							arg1 = ctxt.mkIntConst(PostgresVisitor.asString(right));
						else
							arg1 = ctxt.mkIntConst(PostgresVisitor.asExpectedValues(right));
						if(leftIsVar)
							arg0 = ctxt.mkIntConst(PostgresVisitor.asString(left));
						else
							arg0 = ctxt.mkIntConst(PostgresVisitor.asExpectedValues(left));
						
						BoolExpr e = makeBoolExpr(ctxt, arg0, arg1, getOperator(expr));
						
						if(!sat) {				
							System.err.println(e);
							e = ctxt.mkNot(e);
							System.err.println(e);
						}
						s.add(e);
					}					  				
        		} else {
    				//Expression is not a comparison (!Binary Node)
    				throw new IgnoreMeException();
    			}
    			
    			//check if there is actually a variable to evaluate
    			if(var.equals("both") || var.equals("")) {	
            		//just use random numbers
            		PostgresExpression generateConstant;
                    generateConstant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                            columns.get(i).getType());
		            sb.append(PostgresVisitor.asString(generateConstant));
    			}
    			//variable of the check is not equal to the one to evaluate
    			else {
    				if(!var.equals(currColumn)) {
						//add constraints for the variable to evaluate
						for (Tuple t : currConstraints) {
			        		if(t.varNameIsEqual(var)) {
			        			//add not equal constraint for the given value
			    		     	s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(var), ctxt.mkInt(t.getVal()))));
			        		}
						}
    				}
                	Status state = s.check();
        			//check if statement is satisfiable
                	if(state.toInt() > 0) {        				
						//Special Case EQ/NEQ
	    				boolean isEqSat = getOperator(expr) == Ops.EQUAL && sat;
	    				boolean isNeqNsat = getOperator(expr) == Ops.NOT_EQUAL && !sat;
	    				boolean isSc = isEqSat || isNeqNsat;
	    				if(isSc) {
							if(isEqSat) {
								if(rightIsVar) {
									addEQ(globalState, sb, left,tableNr, var, sat);
								} else if(leftIsVar) {
									addEQ(globalState, sb, right, tableNr, var, sat);
								}
		    				} else if(isNeqNsat) {
		    					if(rightIsVar) {
		    						addEQ(globalState, sb, left, tableNr, var, sat);
								} else if(leftIsVar) {
									addEQ(globalState, sb, right, tableNr, var, sat);
								}
	    					}
	    				//Standard case
	    				} else {
	    					Model model = s.getModel();
	        				Expr e = model.eval(ctxt.mkIntConst(var), true);
	        				long x = modelToLong(e);
	        				s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(currColumn), ctxt.mkInt(x))));
	        				//System.err.println("name | value: " + name + " | " + ctxt.mkInt(x));
	        				PostgresExpression generateConstantFromZ3 = PostgresConstant.createIntConstant(x);
	        				if(sat)
	        					globalState.addUsedNumberSat(tableNr, var, x);
	        				else
	        					globalState.addUsedNumberNsat(tableNr, var, x);
	
	        				sb.append(PostgresVisitor.asString(generateConstantFromZ3));
	    				}
	                	//not satisfiable
            		} else {
	            		throw new IgnoreMeException();
	            	}
    			}    		
            } else {
                sb.append("DEFAULT");
            }
            
        }
        sb.append(")");

    }

    
    private static boolean isVar(PostgresExpression expr) {
    	String var = PostgresVisitor.asString(expr);
    	return var.contains("t");
    }

    
    private static BoolExpr makeBoolExpr(Context ctxt, ArithExpr arg0, ArithExpr arg1, Ops op) {
    	//add correct constraint according to operator
    	BoolExpr constraint;
		switch (op) {
		case GREATER_EQUAL:
			return constraint = ctxt.mkGe(arg0, arg1);				
		case GREATER_THAN:
			return constraint = ctxt.mkGt(arg0, arg1);			
		case LESS_EQUAL:
			return constraint = ctxt.mkLe(arg0, arg1);
		case LESS_THAN:
			return constraint = ctxt.mkLt(arg0, arg1);				
		case EQUAL:
			return constraint = ctxt.mkEq(arg0, arg1);				
		case NOT_EQUAL:
			return constraint = ctxt.mkNot(ctxt.mkEq(arg0, arg1));
		default:
			throw new IgnoreMeException();
		}
    }
    
    private enum Ops {
    	GREATER_EQUAL, GREATER_THAN, LESS_EQUAL, LESS_THAN, EQUAL, NOT_EQUAL, NOP;
    }
    
    private static Ops getOperator(PostgresExpression expr) {
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
	
    private static long modelToLong(Expr e) throws IgnoreMeException{
    	if(e.isInt()) {
			IntNum eIntSat = (IntNum) e;
			BigInteger y = eIntSat.getBigInteger();
			return y.longValue();
		} else {
			throw new IgnoreMeException();
		}
    }
    private static void addEQ(PostgresGlobalState globalState, StringBuilder sb, PostgresExpression e, int tableNr, String var, boolean sat) {
		//add the constraints value
			sb.append(PostgresVisitor.asString(e));
			if(sat) 
				globalState.addUsedNumberSat(tableNr, var, Long.parseLong(PostgresVisitor.asString(e)));				
			else
				globalState.addUsedNumberNsat(tableNr, var, Long.parseLong(PostgresVisitor.asString(e)));
    }
}
