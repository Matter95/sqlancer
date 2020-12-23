package sqlancer.postgres.gen;

import java.math.BigInteger;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Iterator;
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
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        
        
        sb.append(" VALUES");

        int n = Randomly.smallNumber() + 1;
        for (int i = 0; i < n; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            insertRow(globalState, sb, columns, n == 1, table.getName());
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

	private static void insertRow(PostgresGlobalState globalState, StringBuilder sb, List<PostgresColumn> columns,
            boolean canBeDefault, String tableName) throws IgnoreMeException {
          	
    	
    	sb.append("(");
    	
    	//generate a Z3 solver and initialize it accordingly
    	Context ctxt = new Context();
    	Solver s = ctxt.mkSolver(); 
    	String var = "";
    	
    	int tableNr = getTableNumber(tableName);
        //get the list of checks for the given table
    	ArrayList<PostgresExpression> checks = globalState.getCheckStatementsOfTableN(tableNr);

    	
    	    	
        for (int i = 0; i < columns.size(); i++) {
    		s.reset();
        	if (i != 0) {
                sb.append(", ");
            }
            
            if (!Randomly.getBooleanWithSmallProbability() || !canBeDefault) {
        	
    			PostgresExpression constant;
            	PostgresExpression generateConstantFromZ3;
            	PostgresExpression expr = checks.get(i);
            	
    			if (expr instanceof BinaryNode) {
    				@SuppressWarnings("unchecked")
    				BinaryNode<PostgresExpression> comp = (BinaryNode<PostgresExpression>) expr;
    				PostgresExpression left = comp.getLeft();
    				PostgresExpression right = comp.getRight();
    				
    				if(isVar(left)) {
    					var = PostgresVisitor.asString(left);
    				}
    				if(isVar(right)) {
    					var = PostgresVisitor.asString(right);					
    				}
    				if(isVar(left) && isVar(right)) {
    					var = "both";
    				}
    				//create arguments
    				addConstraints(s, ctxt, left, right, getOperator(expr));
        		}
    			else {
    				//Expression is not a comparison (Binary Node)
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
        		}
        		else {
        				Model model = s.getModel();
        				Expr e = model.eval(ctxt.mkIntConst(var), true);
        				if(e.isInt()) {
        					IntNum eint = (IntNum) e;
        					BigInteger y = eint.getBigInteger();
        					long x = y.longValue();
	                    	generateConstantFromZ3 = PostgresConstant.createIntConstant(x);
	                    	//add the new value as a constraint to the solver, which should prevent the same results
	                    	s.add(ctxt.mkNot(ctxt.mkEq(ctxt.mkIntConst(var), ctxt.mkInt(x))));
        				}
        				else {
        					throw new IgnoreMeException();
        				}
        				constant = generateConstantFromZ3;
                	}   
		            sb.append(PostgresVisitor.asString(constant));
	            	}
	            	//not satisfiable
	            	else {
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
    	}
    	else {
    		arg0 = ctxt.mkInt(PostgresVisitor.asExpectedValues(left));
    	}
    	if(isVar(right)) {
    		arg1 = ctxt.mkIntConst(PostgresVisitor.asString(right));
    	}
    	else {
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
			constraint = ctxt.mkEq(ctxt.mkInt(0), ctxt.mkInt(0));
			break;
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
