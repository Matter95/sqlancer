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
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).sorted().collect(Collectors.joining(", ")));
        sb.append(")");
                
        sb.append(" VALUES");
        
    	//generate a Z3 solver and initialize it accordingly
    	Context ctxt = new Context();
    	Solver s = ctxt.mkSolver(); 
    	
    	//initialize used Numbers
    	globalState.initializeUsedNumbers(globalState.getSchema().getDatabaseTables().size());
    	
        int n = Randomly.smallNumber() + 1;
        for (int i = 0; i < n; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            insertRow(globalState, s, ctxt, sb, columns, n == 1, table);
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

	private static void insertRow(PostgresGlobalState globalState, Solver s, Context ctxt, StringBuilder sb, List<PostgresColumn> columns,
            boolean canBeDefault, PostgresTable table) throws IgnoreMeException {
          	
    	sb.append("(");    	

    	String var = "";
    	int tableNr = getTableNumber(table.getName());
        //get the list of checks for the given table
    	ArrayList<PostgresExpression> checks = globalState.getCheckStatementsOfTableN(tableNr);
    
        for (int i = 0; i < columns.size(); i++) {
        	s.reset();
        	if (i != 0) {
                sb.append(", ");
            }
            
        	//Add all used Numbers to the constraints
        	ArrayList<Tuple> currConstraints = globalState.getUsedNumbers(tableNr);
        	String currColumn = table.getName() + "." + table.getColumns().get(i).getName();
        	//for each Tuple of the given table
        	for (Tuple t : currConstraints) {
        		if(t.varNameIsEqual(currColumn)) {
        			//add not equal constraint with the given value
    		     	s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(currColumn), ctxt.mkInt(t.getVal()))));
        		}
			}

        	
            if (!Randomly.getBooleanWithSmallProbability() || !canBeDefault) {
        	
    			PostgresExpression constant;
            	PostgresExpression expr = checks.get(i);
            	
    			if (expr instanceof BinaryNode) {
    				@SuppressWarnings("unchecked")
    				BinaryNode<PostgresExpression> comp = (BinaryNode<PostgresExpression>) expr;
    				PostgresExpression left = comp.getLeft();
    				PostgresExpression right = comp.getRight();
    				boolean hasVar = false;
    				
    				if(isVar(left) && isVar(right)) {
    					var = "both";
    				} else if(isVar(left)) {
    					var = PostgresVisitor.asString(left);
    					hasVar = true;
    				} else if(isVar(right)) {
    					var = PostgresVisitor.asString(right);	
    					hasVar = true;
    				}
    				
    				//create argument when a variable is present
    				if(hasVar)
    					addConstraints(s, ctxt, left, right, getOperator(expr));
        		} else {
    				//Expression is not a comparison (!Binary Node)
    				throw new IgnoreMeException();
    			}
        			
    			//check if statement is satisfiable
            	Status state = s.check();
            	if (state.toInt() > 0) {
        			//check if there is actually a variable to evaluate
        			if(var.equals("both") || var.equals("")) {	
                		//just use random numbers
                		PostgresExpression generateConstant;
                        generateConstant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                                columns.get(i).getType());
                        
                        constant = generateConstant;
                    //evaluate variable
        			} else {
        				Expr e = s.getModel().eval(ctxt.mkIntConst(var), true);
        				if(e.isInt()) {
        					IntNum eint = (IntNum) e;
        					BigInteger y = eint.getBigInteger();
        					long x = y.longValue();
        					PostgresExpression generateConstantFromZ3 = PostgresConstant.createIntConstant(x);
        					//System.err.println("var: " + var + " val: " + x);
        				    //System.err.println(s.toString());
        					globalState.addUsedNumber(tableNr, var, x);
	                    	
	        				constant = generateConstantFromZ3;
        				} else {
        					throw new IgnoreMeException();
        				}
                	}   
		            sb.append(PostgresVisitor.asString(constant));
	            	
	            //not satisfiable
            	} else {
	            		throw new IgnoreMeException();
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

    
    private static void addConstraints(Solver s, Context ctxt, PostgresExpression left, PostgresExpression right, ops op) {
    	ArithExpr arg0;
    	ArithExpr arg1;
    	
    	//create named Constant or Integer Constant
    	if(isVar(left)) {
    		arg0 = ctxt.mkIntConst(PostgresVisitor.asString(left));
    	} else {
    		arg0 = ctxt.mkInt(PostgresVisitor.asExpectedValues(left));
    	}
    	
    	if(isVar(right)) {
    		arg1 = ctxt.mkIntConst(PostgresVisitor.asString(right));
    	} else {
    		arg1 = ctxt.mkInt(PostgresVisitor.asExpectedValues(right));
    	}    
    	
    	//add correct constraint according to operator
    	BoolExpr constraint;
    	switch (op) {
		case GREATER_EQUAL:
			constraint = ctxt.mkGe(arg0, arg1);
			break;
		case GREATER_THAN:
			constraint = ctxt.mkGt(arg0, arg1);
			break;
		case LESS_EQUAL:
			constraint = ctxt.mkLe(arg0, arg1);
			break;
		case LESS_THAN:
			constraint = ctxt.mkLt(arg0, arg1);
			break;
		case EQUAL:
			constraint = ctxt.mkEq(arg0, arg1);
			break;
		case NOT_EQUAL:
			constraint = ctxt.mkNot(ctxt.mkEq(arg0, arg1));
			break;
		default:
			throw new IgnoreMeException();
		}
    	s.add(constraint);
    }
    
    private enum ops {
    	GREATER_EQUAL, GREATER_THAN, LESS_EQUAL, LESS_THAN, EQUAL, NOT_EQUAL, NOP;
    }
    
    private static ops getOperator(PostgresExpression expr) {
    	String check = PostgresVisitor.asString(expr);
    	String ge = ">=";
    	String gt = ">";
    	String le = "<=";
    	String lt = "<";
    	String eq = "=";
    	String neq = "!=";

    	
    	if(check.contains(ge)) {
    		return ops.GREATER_EQUAL;
    	}
    	else if(check.contains(gt)) {
    		return ops.GREATER_THAN;
    	}
    	else if(check.contains(le)) {
    		return ops.LESS_EQUAL;
    	}
    	else if(check.contains(lt)) {
    		return ops.LESS_THAN;
    	}
    	else if(check.contains(neq)) {
    		return ops.NOT_EQUAL;
    	}
    	else if(check.contains(eq)) {
    		return ops.EQUAL;
    	}
    	else {
    		return ops.NOP;
    	}
    }
    
    private static int getTableNumber(String tableName) {
    	return Integer.parseInt(tableName.substring(1));
    }
		
}
