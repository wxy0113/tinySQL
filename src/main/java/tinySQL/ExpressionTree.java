package main.java.tinySQL;

import main.java.storageManager.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

public class ExpressionTree {
    private ArrayList<ExpressionTreeNode> roots;
    private Stack<String> operators;
    private Stack<ExpressionTreeNode> operands;
    private ArrayList<Integer> AndOrIndex;
    private ArrayList<String> AndOrNots;
    public ArrayList<String[]> subconditions;
    public ArrayList<Pair<ArrayList<String>, String>> natureJoin;

    ExpressionTree(String[] conditions) {
        this.roots = new ArrayList<>();
        this.operators = new Stack<>();
        this.operands = new Stack<>();
        this.AndOrIndex = new ArrayList<>();
        this.AndOrNots = new ArrayList<>();
        this.subconditions = new ArrayList<>();
        this.natureJoin = new ArrayList<>();

        splitConditions(conditions);

        if (AndOrIndex.size() != 0) {
            for (String[] con : subconditions) {
                //System.out.println(Arrays.asList(con));
                roots.add(build(con));
            }
        } else {
            roots.add(build(conditions));
        }

        if (subconditions.size() != 0) {
            for (String[] sub : subconditions) {
                checkNaturalJoin(sub);
            }
        } else {
            checkNaturalJoin(conditions);
        }
    }

    ExpressionTree() {
        this.roots = new ArrayList<>();
        this.operators = new Stack<>();
        this.operands = new Stack<>();
        this.AndOrIndex = new ArrayList<>();
        this.AndOrNots = new ArrayList<>();
        this.subconditions = new ArrayList<>();
    }

    public void splitConditions(String[] conditions) {
        for (int i = 0; i < conditions.length; i++) {
            String s = conditions[i];
            if (s.equalsIgnoreCase("AND")) {
                AndOrNots.add("&");
                AndOrIndex.add(i);
            } else if (s.equalsIgnoreCase("OR")) {
                AndOrNots.add("|");
                AndOrIndex.add(i);
            } else if (s.equalsIgnoreCase("NOT")) {
                AndOrNots.add("!");
            }
        }
        if (AndOrIndex.size() != 0) {
            String[] sub = Arrays.copyOfRange(conditions, 0, AndOrIndex.get(0));
            subconditions.add(sub);

            for (int i = 0; i < AndOrIndex.size()-1; i++) {
                sub = Arrays.copyOfRange(conditions, AndOrIndex.get(i), AndOrIndex.get(i+1));
                subconditions.add(sub);
            }
            sub = Arrays.copyOfRange(conditions, AndOrIndex.get(AndOrIndex.size()-1)+1, conditions.length);
            subconditions.add(sub);
        }
    }

    public ExpressionTreeNode build(String[] conditions) {
        ExpressionTreeNode node = new ExpressionTreeNode();

        if (conditions[0].equalsIgnoreCase("NOT")) {
            conditions = Arrays.copyOfRange(conditions, 1, conditions.length);
        }

        for (String con : conditions) {
            if (con.equals("(")) {
                processLeftParenthesis();
            } else if (con.equals(")")) {
                processRightParenthesis();
            } else if (validOperator(con)) {
                processOperator(con);
            } else {
                processOperand(con);
            }
        }

        while (!operators.isEmpty()) {
            connectNode();
        }

        node = operands.pop();
        return node;
    }

    public void processLeftParenthesis() {
        operators.push("(");
    }

    public void processRightParenthesis() {
        while (!operators.isEmpty() && !operators.peek().equals("(")) {
            connectNode();
        }
        operators.pop();
    }

    public boolean validOperator(String s) {
        final String[] operator =  new String[] {"+", "-", "*", "/", ">", "<", "&", "|", "!", "="};
        if (Arrays.asList(operator).contains(s)) return true;
        return false;
    }
    public void processOperator(String s) {
        int order = getPreference(s);
        while (!operators.isEmpty() && order <= getPreference(operators.peek())) {
            connectNode();
        }
        operators.push(s);
    }

    public void processOperand(String s) {
        operands.push(new ExpressionTreeNode(s));
    }

    public void connectNode() {
        String op = operators.pop();
        ExpressionTreeNode right = operands.pop();
        ExpressionTreeNode left = operands.pop();

        ExpressionTreeNode node = new ExpressionTreeNode(op, right, left);
        operands.push(node);
    }

    public int getPreference(String s) {
        final String[] s1 = new String[] {"*", "/"};
        final String[] s2 = new String[] {"+", "-", ">", "<"};

        if (Arrays.asList(s1).contains(s)) {
            return 2;
        } else if (Arrays.asList(s2).contains(s)){
            return 1;
        } else if (s.equals("=")) {
            return 0;
        } else {
            return -1;
        }
    }

    public void printTree() {
        for (ExpressionTreeNode node : roots) {
            print(node);
        }
    }
    public void print(ExpressionTreeNode node) {
        node.print();
        if (node.left != null) print(node.left);

        if (node.right != null) print(node.right);
    }

    public boolean checkTuple(Tuple tuple) {
        ArrayList<String> res = new ArrayList<>();

        for (int i = 0; i < roots.size(); i++) {
            res.add(evaluate(tuple, roots.get(i)));
        }

        for (String s : res) {
            //System.out.print(s + " ");
        }

        for (int i = 0; i < AndOrNots.size(); i++) {
            if (AndOrNots.get(i).equalsIgnoreCase("NOT")) {
                if (res.get(i).equalsIgnoreCase("true")) {
                    res.set(i, "false;");
                } else {
                    res.set(i, "true");
                }
            }
        }

        int j = 0;
        for (int i = 0; i < AndOrNots.size(); i++) {
            String con = AndOrNots.get(i);
            if (con.equalsIgnoreCase("or")) {
                if (res.get(j).equalsIgnoreCase("true") || res.get(j+1).equalsIgnoreCase("true")) {
                    res.set(j, "true");
                    res.set(j+1, "true");
                }
            }
            if (con.equalsIgnoreCase("and")) {
                if (!res.get(j).equalsIgnoreCase("true") || !res.get(j+1).equalsIgnoreCase("true")) {
                    res.set(j, "false");
                    res.set(j+1, "false");
                }
            }
            j++;
        }
        return res.get(res.size()-1).equalsIgnoreCase("true");
    }

    public String evaluate(Tuple tuple, ExpressionTreeNode node) {
        String nodeOp = node.val;
        String leftOp, rightOp;
        switch (nodeOp) {
            case "+":
                leftOp = evaluate(tuple, node.left);
                rightOp = evaluate(tuple, node.right);
                return String.valueOf(Integer.parseInt(leftOp)+Integer.parseInt(rightOp));
            case "-":
                leftOp = evaluate(tuple, node.left);
                rightOp = evaluate(tuple, node.right);
                return String.valueOf(Integer.parseInt(rightOp)-Integer.parseInt(leftOp));
            case "*":
                leftOp = evaluate(tuple, node.left);
                rightOp = evaluate(tuple, node.right);
                return String.valueOf(Integer.parseInt(leftOp)*Integer.parseInt(rightOp));
            case "/":
                leftOp = evaluate(tuple, node.left);
                rightOp = evaluate(tuple, node.right);
                return String.valueOf(Integer.parseInt(rightOp)/Integer.parseInt(leftOp));
            case ">":
                leftOp = evaluate(tuple, node.left);
                rightOp = evaluate(tuple, node.right);
                return String.valueOf(Integer.parseInt(rightOp)>Integer.parseInt(leftOp));
            case "<":
                leftOp = evaluate(tuple, node.left);
                rightOp = evaluate(tuple, node.right);
                return String.valueOf(Integer.parseInt(rightOp)<Integer.parseInt(leftOp));
            case "=":
                leftOp = evaluate(tuple, node.left);
                rightOp = evaluate(tuple, node.right);
                if (validInteger(leftOp) && validOperator(rightOp)) {
                    return String.valueOf(Integer.parseInt(leftOp)==Integer.parseInt(rightOp));
                } else {
                    return String.valueOf(leftOp.equals(rightOp));
                }
            default:
                if (validInteger(nodeOp)) {
                    return nodeOp;
                } else {
                    if (tuple.getSchema().fieldNamesToString().contains(nodeOp)) {
                        return tuple.getField(nodeOp).toString();
                    } else {
                        nodeOp = nodeOp.replaceAll("\"", "");
                        return nodeOp;
                    }
                }
        }
    }

    private void checkNaturalJoin(String[] condition){
        if(condition.length==3){
            if(condition[1].equals("=") && condition[0].contains(".") && condition[2].contains(".")){
                if(condition[0].split("\\.")[1].equals(condition[2].split("\\.")[1])) {
                    ArrayList<String> temp = new ArrayList<String>();
                    temp.add(condition[0].split("\\.")[0]);
                    temp.add(condition[2].split("\\.")[0]);
                    Pair nature = new Pair<>(temp, condition[0].split("\\.")[1]);
                    this.natureJoin.add(nature);
                }
            }
        }
    }

    public static boolean validInteger(String s) {
        return s.matches("\\d+");
    }
}



























