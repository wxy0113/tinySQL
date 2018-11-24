package main.java.tinySQL;

import java.util.ArrayList;

public class ParseTreeNode {
    public String type;
    public String selectList;
    public String orderBy;

    public boolean distinct;
    public boolean where;
    public boolean order;

    public ArrayList<String> attributes;
    public ArrayList<String> tableList;

    public int fromID;
    public int whereID;
    public int orderID;
    public int distID;

    ParseTreeNode(String s){
        this.type = s;
        this.distinct = false;
        this.where = false;
        this.attributes = new ArrayList<String>();
        this.tableList = new ArrayList<String>();
    }
}
