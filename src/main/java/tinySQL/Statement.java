package main.java.tinySQL;
import main.java.storageManager.*;

import java.util.ArrayList;
import java.util.List;


public class Statement {
    public String tableName;
    public ArrayList<String> attributesList;
    public ArrayList<FieldType> typesList;
    public ArrayList<List<String>> valuesList;
    public ParseTreeNode node;
    public Statement(){
        attributesList = new ArrayList<>();
        typesList = new ArrayList<>();
        valuesList = new ArrayList<>();
    }
}
