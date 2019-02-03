package main.java.tinySQL;
import main.java.storageManager.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
    // Validate
    public boolean validCreate(String sql) {
        return true;
    }
    public boolean validDrop(String sql) {
        return true;
    }
    public boolean validSelect(String sql) {
        return true;
    }
    public boolean validInsert(String sql) {
        return true;
    }
    public boolean validDelete(String sql) {
        return true;
    }

    // Parse
    public Statement parseCreate(String sql) {
        /*
            Parse "CREATE"
            i.e. CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)
         */
        if (!validCreate(sql)) {
            System.out.println("Not Valid sql!");
            return null;
        }
        Statement state = new Statement();
        Pattern p = Pattern.compile("CREATE[\\s]+TABLE[\\s]+(.+)[\\s]+\\((.*)\\)");
        Matcher m = p.matcher(sql);
        /*
            m.group(0): CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)
            m.group(1): course
            m.group(2): sid INT, homework INT, project INT, exam INT, grade STR20
         */
        while(m.find()){
            state.tableName = m.group(1);
            String[] temp = m.group(2).split(",");
            for(String s : temp){
                s = s.trim();
                String[] name_type = s.split(" ");
                state.attributesList.add(name_type[0]);
                if(name_type[1].equalsIgnoreCase("INT")){
                    state.typesList.add(FieldType.INT);
                }else{
                    state.typesList.add(FieldType.STR20);
                }
            }
        }
        /*
            state.tableName: persons
            state.fieldTypes: [INT, INT, INT, INT, STR20]
            state.fieldNames: [sid, homework, project, exam, grade]
         */
        return state;
    }

    public String parseDrop(String sql) {
        /*
            Parse "DROP"
            i.e. DROP TABLE course
         */
        if (!validDrop(sql)) {
            System.out.println("Not Valid sql!");
            return null;
        }
        Pattern p = Pattern.compile("DROP[\\s]+TABLE[\\s]+(.+)");
        Matcher m = p.matcher(sql);
        /*
            m.group(0): DROP TABLE course
            m.group(1): course
         */
        while (m.find()) {
//            System.out.println(m.group(0));
//            System.out.println(m.group(1));
            return m.group(1);
        }
        return null;
    }

    public Statement parseSelect(String sql) {
        // Parse "SELECT"
        // i.e. SELECT * FROM course WHERE exam = 90 AND project = 100
        if (!validSelect(sql)) {
            System.out.println("Not Valid sql!");
            return null;
        }
        String[] strs = sql.trim().toLowerCase().replaceAll("\\[","").replaceAll("\\]","").replaceAll(",", "").split("\\s+");
        // strs = [SELECT, *, FROM, course, WHERE, exam, =, 90, AND, project, =, 100]
        //System.out.println(Arrays.asList(strs));
        Statement state = new Statement();
        ParseTreeNode node = new ParseTreeNode("select");

        for (int i = 0; i < strs.length; i++) {
            String s = strs[i];
            // Handle distinct
            if (s.equalsIgnoreCase("DISTINCT")) {
                node.distinct = true;
                node.distID = i;
            }
            // Handle from
            if (s.equalsIgnoreCase("FROM")) {
                node.fromID = i;
            }
            // Handle where
            if (s.equalsIgnoreCase("WHERE")) {
                node.where = true;
                node.whereID = i;
            }
            // Handle order by
            if (i < strs.length-1) {
                if (s.equalsIgnoreCase("ORDER") && strs[i+1].equalsIgnoreCase("BY")) {
                    node.order = true;
                    node.orderID = i;
                }
            }
        }
        if (node.distinct) {
            node.selectList = strs[node.distID+1];
            node.attributes = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(strs, 2, node.fromID)));
        } else {
            node.attributes = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(strs, 1, node.fromID)));
        }
        if (node.where) {
            node.tableList = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(strs, node.fromID+1, node.whereID)));
            if (node.order) {
                String[] searchCondition = Arrays.copyOfRange(strs, node.whereID+1, node.orderID);
                // Expression Tree
                node.expressionTree = new ExpressionTree(searchCondition);
                //node.expressionTree.printTree();
                node.orderBy = strs[strs.length-1];
                //System.out.println(searchCondition);
                // After ExpressionTree
                /*
                       =              =
                     /    \         /   \
                    exam   90   project  100
                 */
            } else {
                String[] searchCondition = Arrays.copyOfRange(strs, node.whereID+1, strs.length);
                node.expressionTree = new ExpressionTree(searchCondition);
                //node.expressionTree.printTree();
                //System.out.println(node.expressionTree.subconditions);
            }
        } else {
            if (node.order) {
                node.tableList = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(strs, node.fromID+1, node.orderID)));
            } else {
                node.tableList = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(strs, node.fromID+1, strs.length)));
            }
        }
        if (node.order) {
            node.orderBy = strs[strs.length-1];
        }
        state.node = node;
        //System.out.println(node.tableList);
        //System.out.println(node.attributes);
        return state;
    }

    public Statement parseInsert(String sql) {
        /*
            Parse "INSERT"
            i.e. INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
         */
        if (!validInsert(sql)) {
            System.out.println("Not Valid sql!");
            return null;
        }
        Statement state = new Statement();
        Pattern p = Pattern.compile("INSERT[\\s]+INTO[\\s]+(.+)[\\s]+\\((.*)\\)[\\s]+VALUES[\\s]+\\((.*)\\)");
        Matcher m = p.matcher(sql);
        /*
            m.group(0): INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
            m.group(1): course
            m.group(2): sid, homework, project, exam, grade
            m.group(3): 1, 99, 100, 100, "A"
         */
        while (m.find()) {
//            System.out.println(m.group(0));
//            System.out.println(m.group(1));
//            System.out.println(m.group(2));
//            System.out.println(m.group(3));
            state.tableName = m.group(1);
            String[] temp = m.group(2).split(",");
            for(String s : temp){
                s = s.trim();
                state.attributesList.add(s.split(" ")[0]);
            }
            List<String> list = new ArrayList<>();
            String[] values = m.group(3).replaceAll("\\(", "").replaceAll("\\)", "").split(",");
            int len = state.attributesList.size();
            for (int i = 0; i < values.length; i++) {
                values[i] = values[i].trim();
                if(values[i].charAt(0) == '\"' && values[i].charAt(values[i].length() - 1) == '\"'){
                    values[i] = values[i].replace("\"", "");
                }
                list.add(values[i]);
                if ((i+1)%len == 0) {
                    state.valuesList.add(new ArrayList<>(list));
                    list.clear();
                }
            }
        }
//        System.out.println(state.fieldNames);
//        System.out.println(state.fieldTypes);
//        System.out.println(state.tableName);
//        System.out.println(state.fieldValues);
        /*
            state.fieldNames: [1, 99, 100, 100, A]
            state.fieldTypes: [sid, homework, project, exam, grade]
            state.tableName: course
            state.fieldValues: [[1, 99, 100, 100, A]]
         */
        return state;
    }

    public Statement[] parseInsertForSelect(String sql) {
        String temp = sql.trim().toLowerCase();
        sql = sql.trim();
        Statement[] state = new Statement[2];
        int second = temp.indexOf("select");
        String insert = sql.substring(0, second);
        String select = sql.substring(second);
        //String[] splitResult = insert.split("[\\s]+");
        state[0] = new Statement();
        Pattern p = Pattern.compile("INSERT[\\s]+INTO[\\s]+(.+)[\\s]+\\((.*)\\)[\\s]");
        Matcher m = p.matcher(insert);
        while (m.find()) {
            System.out.println(m.group(0));
            System.out.println(m.group(1));
            System.out.println(m.group(2));
            state[0].tableName = m.group(1);
            String[] tempList = m.group(2).split(",");
            for(String s : tempList){
                s = s.trim();
                state[0].attributesList.add(s.split(" ")[0]);
            }
        }
        state[1] = parseSelect(select.trim());
        //String tableName = splitResult[2];
        //ParseTreeNode parserTree = parseSelect(select).node;
//        String[] splitResult = sql.split("[\\s]+", 4);
//        state.tableName = splitResult[2];
//        String selectCondition = splitResult[3];
//        state.node = parseSelect(selectCondition).node;
        return state;
    }

    public Statement parseDelete(String sql) {
        if (!validDelete(sql)) {
            System.out.println("Not Valid sql!");
            return null;
        }
        String[] strs = sql.trim().replaceAll("[,\\s]", ",").split(",");
        ParseTreeNode node = new ParseTreeNode("delete");
        Statement state = new Statement();

        for (int i = 0; i < strs.length; i++) {
            String s = strs[i];
            if (s.equalsIgnoreCase("FROM")) {
                node.fromID = i;
            }
            if (s.equalsIgnoreCase("WHERE")) {
                node.where = true;
                node.whereID = i;
            }
        }

        if (node.where) {
            node.tableList = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(strs, node.fromID+1, node.whereID)));
            String[] searchCondition = Arrays.copyOfRange(strs, node.whereID+1, strs.length);
            node.expressionTree = new ExpressionTree(searchCondition);
            //node.expressionTree.printTree();
            //System.out.println(node.expressionTree.subconditions);
        } else {
            node.tableList = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(strs, node.fromID+1, strs.length)));
        }
        state.node = node;
        return state;
    }

    public static void main(String[] args) {
        String test1 = "CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20) ";
        String test2 = "DROP TABLE course";
        String test3 = "INSERT INTO course (sid, homework, project, exam, grade)";
        String test4 = "INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course";
        String test5 = "DELETE FROM course WHERE grade = \"E\"";
        String test6 = "SELECT * FROM course";
        Parser p = new Parser();
        //
        //p.parseCreate(test1);
        //p.parseDrop(test2);
        //p.parseInsert(test3);
        //p.parseInsertForSelect(test4);
        //p.parseDelete(test5);
        p.parseSelect(test6);
    }
}
