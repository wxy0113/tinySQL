package main.java.tinySQL;

public class ExpressionTreeNode {
    public String val;
    public ExpressionTreeNode left, right;
    public ExpressionTreeNode() {
        this.val = null;
        this.left = null;
        this.right = null;
    }
    public ExpressionTreeNode(String val) {
        this.val = val;
        this.left = null;
        this.right = null;
    }
    public ExpressionTreeNode(String val, ExpressionTreeNode left, ExpressionTreeNode right) {
        this.val = val;
        this.left = left;
        this.right = right;
    }

    public void print() {
        System.out.println(val);
    }
}
