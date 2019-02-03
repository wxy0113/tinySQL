package main.java.tinySQL;

import main.java.storageManager.*;

import javax.swing.plaf.nimbus.State;
import java.util.*;

class Pair<A, B> {
    public A first;
    public B second;
    public Pair( ){};
    public Pair(A name, B storage) {
        this.first = name;
        this.second = storage;
    }
}

public class PhysicalQuery {
    private Parser parser;
    private static MainMemory mainMemory;
    private Disk disk;
    private SchemaManager schemaManager;

    public PhysicalQuery() {
        parser = new Parser();
        mainMemory = new MainMemory();
        disk = new Disk();
        schemaManager = new SchemaManager(mainMemory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public void execute(String s) {
        //s = s.toLowerCase();
        //mainMemory.getTuples(0, 0);
        double simu_time = disk.getDiskTimer();
        long simu_IO = disk.getDiskIOs();
        String command  = s.trim().toLowerCase().split("\\s")[0];
//        try {
//            System.out.println("Executing Query: "+s);
//            switch (command) {
//                case "create":
//                    createQuery(s); break;
//                case "select":
//                    selectQuery(s); break;
//                case "drop":
//                    dropQuery(s); break;
//                case "delete":
//                    deleteQuery(s); break;
//                case "insert":
//                    insertQuery(s); break;
//                default:
//                    System.out.println("Invalid Command! ");
//            }
//        } catch (Exception e) {
//            System.out.println(e.getStackTrace().toString());
//            System.out.println("Invalid Command: " + s);
//        }
        System.out.println("Executing Query: "+s);
        switch (command) {
            case "create":
                createQuery(s); break;
            case "select":
                selectQuery(s); break;
            case "drop":
                dropQuery(s); break;
            case "delete":
                deleteQuery(s); break;
            case "insert":
                insertQuery(s); break;
            default:
                System.out.println("Invalid Command! ");
        }
        System.out.printf("Simulated processing time = %.2f ms\n"  ,(disk.getDiskTimer()-simu_time));
        System.out.println("Simulated Disk I/Os = " + (disk.getDiskIOs()-simu_IO));
        System.out.println("\n");
    }

    public void createQuery(String sql) {
        Statement state = parser.parseCreate(sql);
        Schema schema = new Schema(state.attributesList, state.typesList);
        schemaManager.createRelation(state.tableName, schema);
        System.out.println("Create Complete!");
    }

    public void dropQuery(String sql) {
       schemaManager.deleteRelation(parser.parseDrop(sql));
    }

    public void deleteQuery(String sql) {
        Statement state = parser.parseDelete(sql);
        ParseTreeNode parseTreeNode = state.node;
        Relation relation = schemaManager.getRelation(parseTreeNode.tableList.get(0));
        int numBlocks = relation.getNumOfBlocks();
        int numMemo = mainMemory.getMemorySize();
        int cur = 0;

        while (cur < numBlocks) {
            //int numBlockMemo = Math.min(numBlocks-cur, numMemo);
            if (numBlocks - cur < numMemo) {
                relation.getBlocks(cur, 0, numBlocks - cur);
                deleteQuery2(relation, mainMemory, parseTreeNode, cur, numBlocks - cur);
                break;
            } else {
                relation.getBlocks(cur, 0, numMemo);
                deleteQuery2(relation, mainMemory, parseTreeNode, cur, numMemo);
                cur += numMemo;
            }
        }
        fillHoles(relation, mainMemory);
        //System.out.println(relation);
        //System.out.println(mainMemory);
        System.out.println("Delete Complete!");
    }
    public void fillHoles(Relation relation, MainMemory memory){
        int numOfBlocks = relation.getNumOfBlocks();
        int sortedBlocks = 0;
        ArrayList<Tuple> tuples;
        while(sortedBlocks < numOfBlocks){
            int t = Math.min(memory.getMemorySize(), numOfBlocks - sortedBlocks);
            relation.getBlocks(sortedBlocks, 0, t);
            tuples = onePassSort(memory, t);
            if (tuples.size() == 0) {
                relation.deleteBlocks(sortedBlocks);
            } else {
                memory.setTuples(0, tuples);
                relation.setBlocks(sortedBlocks, 0, t);
                if (tuples.size() < t) {
                    relation.deleteBlocks(sortedBlocks+tuples.size());
                }
            }
            if(t < memory.getMemorySize()) {
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }
            clearMainMemory();
        }
        //relation.deleteBlocks(sortedBlocks);
        clearMainMemory();
    }
    public void deleteQuery2(Relation relation, MainMemory mainMemory, ParseTreeNode parseTreeNode, int index, int num) {
        for (int i = 0; i < num; i++) {
            Block block = mainMemory.getBlock(i);
            if (block.getNumTuples() == 0) continue;
            ArrayList<Tuple> tuples = block.getTuples();
            if (parseTreeNode.where) {
                for (int j = 0; j < tuples.size(); j++) {
                    if (parseTreeNode.expressionTree.checkTuple(tuples.get(j))) {
                        block.invalidateTuple(j);
                    }
                }
            } else {
                block.invalidateTuples();
            }
        }
        relation.setBlocks(index, 0, num);
        clearMainMemory();
    }

    public void insertQuery(String sql) {
        if (sql.trim().toLowerCase().indexOf("select") == -1) {
            insertQueryForSingleTuple(sql);
        } else {
            insertQueryForSelect(sql);
        }
    }

    public void insertQueryForSingleTuple(String sql) {
        clearMainMemory();
        Statement state = parser.parseInsert(sql);
        Schema schema = schemaManager.getSchema(state.tableName);
        Relation relation = schemaManager.getRelation(state.tableName);
        //ArrayList<String> attrs = state.attributesList;
        for (int i = 0; i < state.valuesList.size(); i++) {
            Tuple tuple = relation.createTuple();
            for (int j = 0; j < state.valuesList.get(i).size(); j++) {
                String val = state.valuesList.get(i).get(j);
                if (schema.getFieldType(state.attributesList.get(j)) == FieldType.INT) {
                    // Handle NULL
                    if (val.equals("NULL")) {
                        tuple.setField(state.attributesList.get(j), Integer.MIN_VALUE);
                    }else {
                        tuple.setField(state.attributesList.get(j), Integer.parseInt(val));
                    }
                } else {
                    tuple.setField(state.attributesList.get(j), val);
                }
            }
            appendTuple(tuple, relation, 0);
        }
        System.out.println("Insert Complete!");
        //System.out.println(relation.getNumOfBlocks());
        //System.out.println(relation);
    }

    public void insertQueryForSelect(String sql) {
        Statement[] state = parser.parseInsertForSelect(sql);
        Statement insert = state[0];
        Statement select = state[1];
        ParseTreeNode parseTree = select.node;
        Relation toRelation = schemaManager.getRelation(insert.tableName);
        String fromTable = parseTree.tableList.get(0);
        Relation fromRelation = schemaManager.getRelation(select.node.tableList.get(0));
        ArrayList<String> selectedAttr = parseTree.attributes;
        ArrayList<Tuple> selectedTuples = new ArrayList<>();

        if (fromRelation == null || selectedAttr.size() == 0) return;

        clearMainMemory();
        int relationNumBlocks = fromRelation.getNumOfBlocks();
        int memNumBlocks = mainMemory.getMemorySize();
        ArrayList<String> selectedFieldName = new ArrayList<>();
        if (selectedAttr.size() == 1 && selectedAttr.get(0).equalsIgnoreCase("*")) {
            selectedFieldName = fromRelation.getSchema().getFieldNames();
        } else {
            selectedFieldName = selectedAttr;
        }
        if (!parseTree.distinct && !parseTree.order) {
            for (int i = 0; i < relationNumBlocks; i++) {
                mainMemory.getBlock(0).clear();
                fromRelation.getBlock(i, 0);// read a block from disk to main
                // memory
                Block mainMemoryBlock = mainMemory.getBlock(0);
                if (mainMemoryBlock.getNumTuples() == 0) {
                    continue;
                }
                for (Tuple tuple : mainMemoryBlock.getTuples()) {
                    Tuple newTuple = toRelation.createTuple();// creates an
                    // empty tuple
                    // of the
                    // schema

                    for (int j = 0; j < selectedFieldName.size(); j++) {
                        Field curField = tuple.getField(selectedFieldName.get(j));
                        if (curField.type == FieldType.INT) {
                            newTuple.setField(selectedFieldName.get(j), curField.integer);
                        } else {
                            newTuple.setField(selectedFieldName.get(j), curField.str);
                        }
                    }
                    appendTuple(newTuple, toRelation, 0);
                }
            }
            //.out.println(toRelation);
        }
    }

    public void appendTuple(Tuple tuple, Relation relation, int index) {
        Block block;
        if (relation.getNumOfBlocks() == 0) {
            block = mainMemory.getBlock(index);
            block.clear();
            block.appendTuple(tuple);
            relation.setBlock(0, index);
        } else {
            relation.getBlock(relation.getNumOfBlocks()-1, index);
            block = mainMemory.getBlock(index);
            if (!block.isFull()) {
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks()-1, index);
            } else {
                block.clear();
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks(), index);
            }
        }
    }

    public void selectQuery(String sql) {
        Statement state = parser.parseSelect(sql);
        ParseTreeNode node = state.node;

        if (node.tableList.size() == 1) {
            selectQueryForSingleTable(node);
        } else {
            selectQueryForMultiTable(node);
        }
        System.out.println("Select Complete! ");
    }

    public void selectQueryForSingleTable(ParseTreeNode node) {
        String table = node.tableList.get(0);
        Relation relation = schemaManager.getRelation(table);
        ArrayList<String> temp = new ArrayList<>();
        ArrayList<Tuple> selected = new ArrayList<>();
        ArrayList<String> selectedAttr = new ArrayList<>();

        if (node.distinct) {
            relation = distinct(schemaManager, relation, mainMemory, node.selectList);
            clearMainMemory();
            temp.add(relation.getRelationName());
        }

        if (node.where) {
            relation = select(schemaManager, relation, mainMemory, node);
            clearMainMemory();
            temp.add(relation.getRelationName());
        }

        if (node.order) {
            relation = sort(schemaManager, relation, mainMemory, node.orderBy);
            clearMainMemory();
            temp.add(relation.getRelationName());
        }

        project2(relation, mainMemory, node, selected, selectedAttr);

        if (temp.size() == 0) return;
        for (String s : temp) {
            //System.out.println(s);
            if (schemaManager.relationExists(s)) schemaManager.deleteRelation(s);
        }
    }
    public void project2(Relation relation, MainMemory memory, ParseTreeNode parseTree, ArrayList<Tuple> selected, ArrayList<String> selectedAttr){
        int numOfBlocks = relation.getNumOfBlocks();
        //System.out.println(relation);
        System.out.println("Relation Blocks Number: " + numOfBlocks);
        //System.out.println(memory);
        System.out.println("Memory Blocks Number: " + memory.getMemorySize());
        int i = 0;
        if (parseTree.attributes.get(0).equals("*")){
            selectedAttr.addAll(relation.getSchema().getFieldNames());
            //System.out.print(relation.getSchema().getFieldNames());
        } else {
            for (int m = 0; m < parseTree.attributes.size(); m++) {
                String att = parseTree.attributes.get(m);
                int k = att.indexOf(".");
                if (k != -1) {
                    att = att.substring(k+1);
                }
                selectedAttr.add(att);
                //System.out.print(parseTree.attributes.get(i) + " ");
            }
        }
        System.out.println("Here is an output");
        //System.out.println(selectedAttr);
        while (i < numOfBlocks) {
            //System.out.println("here is an output");
            int t = Math.min(memory.getMemorySize(), numOfBlocks - i);
            relation.getBlocks(i, 0, t);
            if (!memory.getBlock(0).isEmpty()) {
                projectHelper(relation, memory, parseTree, t, selected, selectedAttr);
                if (t < memory.getMemorySize()) {
                    break;
                }
                //System.out.println(i);
            } else {
                System.out.println("No Selected Tuples");
                return;
            }
            i += t;
        }
        for (Tuple t : selected){
            System.out.println(t);
        }
    }

    public void project(Relation relation, MainMemory memory, ParseTreeNode parseTree, ArrayList<Tuple> selected, ArrayList<String> selectedAttr){
        int numOfBlocks = relation.getNumOfBlocks();
        //System.out.println(relation);
        System.out.println("Relation Blocks Number: " + numOfBlocks);
        //System.out.println(memory);
        System.out.println("Memory Blocks Number: " + memory.getMemorySize());
//        for (int i = 0; i < numOfBlocks; i+=10) {
//            System.out.println("here is an output");
//            int t = Math.min(memory.getMemorySize(), numOfBlocks - i);
//            relation.getBlocks(i, 0, t);
//            if (!memory.getBlock(0).isEmpty()) {
//                projectHelper(relation, memory, parseTree, t);
//                if (t <= memory.getMemorySize()) {
//                    break;
//                }
//                System.out.println(i);
//            } else {
//                System.out.println("No Selected Tuples");
//                return;
//            }
//        }
        int i = 0;
        if (parseTree.attributes.get(0).equals("*")){
            selectedAttr.addAll(relation.getSchema().getFieldNames());
            //System.out.print(relation.getSchema().getFieldNames());
        } else {
            for (int m = 0; m < parseTree.attributes.size(); m++) {
                selectedAttr.add(parseTree.attributes.get(m));
                //System.out.print(parseTree.attributes.get(i) + " ");
            }
        }
        System.out.println("Here is an output");
        //System.out.println(selectedAttr);
        while (i < numOfBlocks) {
            //System.out.println("here is an output");
            int t = Math.min(memory.getMemorySize(), numOfBlocks - i);
            relation.getBlocks(i, 0, t);
            if (!memory.getBlock(0).isEmpty()) {
                projectHelper(relation, memory, parseTree, t, selected, selectedAttr);
                if (t < memory.getMemorySize()) {
                    break;
                }
                //System.out.println(i);
            } else {
                System.out.println("No Selected Tuples");
                return;
            }
            i += t;
        }
        for (Tuple t : selected){
            System.out.println(t);
        }
    }

    private void projectHelper(Relation relation, MainMemory memory, ParseTreeNode parseTree, int memBlocks, ArrayList<Tuple> selected, ArrayList<String> selectedAttr){
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (int i = 0; i < memBlocks; i++) {
            if (memory.getBlock(i) != null) {
                tuples.addAll(memory.getBlock(i).getTuples());
            }
        }

        //System.out.println();
        for(int j = 0; j < tuples.size(); j++) {
            if(selectedAttr.get(0).equals("*")){
                //System.out.println(tuples.get(j));
                selected.add(tuples.get(j));
            }else{
                for(String attr : selectedAttr){
                    if(!tuples.get(j).isNull()) {
                        System.out.print(tuples.get(j).getField(attr) + " ");
                    }
                }
                System.out.println();
            }
        }
        clearMainMemory();
    }

    public ArrayList<Tuple> onePassSort(MainMemory memory, int numOfBlocks){
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        if (tuples.size() != 0) {
            tuples.sort(new TupleComparator());
        }
        clearMainMemory();
        return tuples;
    }

    public static void clearMainMemory() {
        for (int i = 0; i < mainMemory.getMemorySize(); i++) {
            mainMemory.getBlock(i).clear();
        }
    }

    public Relation distinct(SchemaManager schemaManager, Relation relation, MainMemory memory, String fieldName){
        String name = relation.getRelationName() + "_distinct_" + fieldName;
        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        ArrayList<Tuple> tuples;
        if(relation.getNumOfBlocks() <= memory.getMemorySize()){
            tuples = onePassRemoveDuplicate(relation, memory, fieldName);
        }else{
            tuples = twoPassRemoveDuplicate(relation, memory, fieldName);
        }
        return createRelationFromTuples(tuples, name, schemaManager, relation, memory);
    }

    public Relation sort(SchemaManager schemaManager, Relation relation, MainMemory memory, String fieldName){
        String name = relation.getRelationName() + "_sortBy_" + fieldName;
        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        ArrayList<Tuple> tuples;
        if(relation.getNumOfBlocks() <= memory.getMemorySize()){
            tuples = onePassSort(relation, memory, fieldName);
        }else{
            tuples = twoPassSort(relation, memory, fieldName);
        }
        return createRelationFromTuples(tuples, name, schemaManager, relation, memory);
    }
    public ArrayList<Tuple> onePassSort(Relation relation, MainMemory memory, String fieldName){
        int numOfBlocks = relation.getNumOfBlocks();
        //System.out.println(relation);
        relation.getBlocks(0, 0, numOfBlocks);
        Block block = memory.getBlock(0);
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (!block.isEmpty()) {
            tuples.addAll(memory.getTuples(0, numOfBlocks));

        }

        if (tuples.size() != 0) {
            tuples.sort(new TupleComparator(fieldName));
        }
        clearMainMemory();
        return tuples;
    }

    public Relation select(SchemaManager schemaMG, Relation relation, MainMemory memory, ParseTreeNode parseTree){
        ArrayList<Tuple> tuples = new ArrayList<>();
        int numOfBlocks = relation.getNumOfBlocks(), memoryBlocks = memory.getMemorySize();

        if(numOfBlocks <= memoryBlocks){
            tuples = selectQueryHelper(parseTree, relation, memory,0, numOfBlocks);
        }else{
            int remainNumber = numOfBlocks;
            int relationindex = 0;
            ArrayList<Tuple> tmp;
            while(remainNumber > memoryBlocks){
                tmp = selectQueryHelper(parseTree, relation, memory, relationindex, memoryBlocks);
                tuples.addAll(tmp);
                remainNumber = remainNumber - memoryBlocks;
                relationindex = relationindex + memoryBlocks;
            }
            tmp = selectQueryHelper(parseTree, relation, memory, relationindex, remainNumber);
            tuples.addAll(tmp);
        }
        String name = relation.getRelationName() + "_select_";
        if(schemaMG.relationExists(name)) schemaMG.deleteRelation(name);
        return createRelationFromTuples(tuples, name, schemaMG, relation, memory);
    }
    private ArrayList<Tuple> selectQueryHelper(ParseTreeNode parseTree, Relation relation, MainMemory memory, int relationIndex, int loop ){
        Block block;
        ArrayList<Tuple> res = new ArrayList<>();
        relation.getBlocks(relationIndex, 0, loop);
        for(int i=0; i<loop; i++){
            block = memory.getBlock(i);
            ArrayList<Tuple> tuples = block.getTuples();
            for(Tuple tuple : tuples){
                if(parseTree.where){
                    if(parseTree.expressionTree.checkTuple(tuple)) res.add(tuple);
                }else{
                    //System.out.println(tuple);
                    res.add(tuple);
                }
            }
        }
        return res;
    }
    public Relation createRelationFromTuples(ArrayList<Tuple> tuples, String name, SchemaManager schemaManager, Relation relation, MainMemory memory){
        Schema schema = relation.getSchema();
        if(schemaManager.relationExists(name)) schemaManager.deleteRelation(name);
        Relation tempRelation = schemaManager.createRelation(name, schema);
        int tupleNumber = tuples.size(),
                tuplesPerBlock = schema.getTuplesPerBlock();
        int tupleBlocks;
        if(tupleNumber < tuplesPerBlock){
            tupleBlocks = 1;
        }else if(tupleNumber >= tuplesPerBlock && tupleNumber % tuplesPerBlock == 0){
            tupleBlocks = tupleNumber / tuplesPerBlock;
        }else{
            tupleBlocks = tupleNumber / tuplesPerBlock + 1;
        }

        int index = 0;
        while(index < tupleBlocks){
            int t = Math.min(memory.getMemorySize(), tupleBlocks - index);
            for(int i = 0; i < t; i++){
                Block block = memory.getBlock(i);
                block.clear();
                for(int j = 0; j< tuplesPerBlock; j++){
                    if(!tuples.isEmpty()){
                        Tuple temp = tuples.get(0);
                        block.setTuple(j, temp);
                        tuples.remove(temp);
                    }else{
                        break;
                    }
                }
            }
            tempRelation.setBlocks(index,0, t);
            if(t < memory.getMemorySize()){
                break;
            }else{
                index += memory.getMemorySize();
            }
        }
        return tempRelation;
    }

    public ArrayList<Tuple> twoPassSort(Relation relation, MainMemory memory, String fieldName){
        //phase 1: making sorted sublists
        twoPassHelper(relation, memory, fieldName);

        //phase 2: merging
        int numOfBlocks = relation.getNumOfBlocks();
        ArrayList<Tuple> res = new ArrayList<>();
        ArrayList<ArrayList<Tuple>> tuples = new ArrayList<>();
        ArrayList<Pair<Integer, Integer>> blockIndexOfSublists = new ArrayList<>();

        //bring in a block from each of the sorted sublists
        for(int i = 0, j = 0; i < numOfBlocks; i += memory.getMemorySize(), j++){
            //initial index must be i + 1
            blockIndexOfSublists.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), numOfBlocks)));
            relation.getBlock(i, j);
            tuples.add(memory.getTuples(j, 1));
        }

        for(int k = 0; k < relation.getNumOfTuples(); ++k){
            for(int i = 0; i < blockIndexOfSublists.size(); ++i){
                //read in the next block from a sublist if its block is exhausted
                if(tuples.get(i).isEmpty() && (blockIndexOfSublists.get(i).first < blockIndexOfSublists.get(i).second)){
                    relation.getBlock(blockIndexOfSublists.get(i).first, i);
                    tuples.set(i, memory.getTuples(i, 1));
                    blockIndexOfSublists.get(i).first++;
                }
            }

            //find the smallest key among the first remaining elements of all the sublists
            ArrayList<Tuple> minTuples = new ArrayList<>();
            for(int j = 0; j < tuples.size(); ++j){
                if(!tuples.isEmpty() && !tuples.get(j).isEmpty()) minTuples.add(tuples.get(j).get(0));
            }
            Tuple minTuple = Collections.min(minTuples, new TupleComparator(fieldName));
            res.add(minTuple);

            //remove the minimum element
            for(int j = 0; j < tuples.size(); ++j){
                if(!tuples.get(j).isEmpty() && tuples.get(j).get(0).equals(minTuple)) tuples.get(j).remove(0);
            }
        }

        //for(Tuple tuple : res) System.out.println(tuple);
        clearMainMemory();
        return res;
    }
    public static void twoPassHelper(Relation relation, MainMemory memory, String fieldName){
        int numOfBlocks = relation.getNumOfBlocks(),  sortedBlocks = 0;
        ArrayList<Tuple> tuples;
        while(sortedBlocks < numOfBlocks){
            int t = Math.min(memory.getMemorySize(), numOfBlocks - sortedBlocks);
            relation.getBlocks(sortedBlocks, 0, t);
            tuples = onePassSort(memory, fieldName, t);
            memory.setTuples(0, tuples);
            relation.setBlocks(sortedBlocks, 0, t);
            if(t < memory.getMemorySize()) {
                break;
            }else{
                sortedBlocks += memory.getMemorySize();
            }
            clearMainMemory();
        }
    }
    public static ArrayList<Tuple> onePassSort(MainMemory memory, String fieldName, int numOfBlocks){
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        tuples.sort(new TupleComparator(fieldName));
        clearMainMemory();
        return tuples;
    }

    public ArrayList<Tuple> onePassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName){
        ArrayList<Tuple> res = new ArrayList<>();
        HashSet<String> hashSet = new HashSet<>();
        //HashMap<String, HashSet> map = new HashMap<>();
        //HashSet<Tuple> set = new HashSet<>();
        int numOfBlocks = relation.getNumOfBlocks();
        relation.getBlocks(0, 0, numOfBlocks);
        ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
        if (fieldName.equalsIgnoreCase("*")) {
            for (Tuple tuple : tuples) {
                String s = tuple.toString();
                if (hashSet.add(s)) res.add(tuple);
            }
        } else {
            //map.put(fieldName, new HashSet<>());
            for(Tuple tuple : tuples){
                if(tuple.getField(fieldName).type.equals(FieldType.STR20)){
                    if(hashSet.add(tuple.getField(fieldName).str)) res.add(tuple);
                }else{
                    if(hashSet.add(Integer.toString(tuple.getField(fieldName).integer))) res.add(tuple);
                }
            }
        }
        clearMainMemory();
        return res;
    }

    public ArrayList<Tuple> twoPassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName){
        //phase 1: making sorted sublists
        twoPassHelper(relation, memory, fieldName);

        //phase 2
        int numOfBlocks = relation.getNumOfBlocks();
        HashSet<String> hashSet = new HashSet<>();
        ArrayList<Tuple> res = new ArrayList<>();
        ArrayList<ArrayList<Tuple>> tuples = new ArrayList<>();
        ArrayList<Pair<Integer, Integer>> blockIndexOfSublists = new ArrayList<>();

        //bring in a block from each of the sorted sublists
        for(int i = 0, j = 0; i < numOfBlocks; i += memory.getMemorySize(), j++){
            //initial index must be i + 1
            blockIndexOfSublists.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), numOfBlocks)));
            relation.getBlock(i, j);
            tuples.add(memory.getTuples(j, 1));
        }

        for(int k = 0; k < relation.getNumOfTuples(); ++k){
            for(int i = 0; i < blockIndexOfSublists.size(); ++i){
                //read in the next block form a sublist if its block is exhausted
                if(tuples.get(i).isEmpty() && (blockIndexOfSublists.get(i).first < blockIndexOfSublists.get(i).second)){
                    relation.getBlock(blockIndexOfSublists.get(i).first, i);
                    tuples.set(i, memory.getTuples(i, 1));
                    blockIndexOfSublists.get(i).first++;
                }
            }

            //find the smallest key among the first remaining elements of all the sublists
            ArrayList<Tuple> minTuples = new ArrayList<>();
            for(int j = 0; j < tuples.size(); ++j){
                if(!tuples.isEmpty() && !tuples.get(j).isEmpty()) minTuples.add(tuples.get(j).get(0));
            }

            //the first difference to twoPassSort -
            //multiple elements could be removed in one loop, the number of loops could be less than numOfBlocks
            //so the loop could break earlier
            if(minTuples.isEmpty()) break;

            Tuple minTuple = Collections.min(minTuples, new TupleComparator(fieldName));
            //the 2nd difference - use Hashset
            if (fieldName.equalsIgnoreCase("*")) {
                if (hashSet.add(minTuple.toString())) res.add(minTuple);
                //the 3rd difference - remove all minimum elements
                for(ArrayList<Tuple> tuple : tuples){
                    if (tuple.toString().equals(minTuple.toString())) tuple.clear();
                }
            } else {
                if(minTuple.getField(fieldName).type.equals(FieldType.STR20)){
                    if(hashSet.add(minTuple.getField(fieldName).str)) res.add(minTuple);
                }else{
                    if(hashSet.add(Integer.toString(minTuple.getField(fieldName).integer))) res.add(minTuple);
                }

                //the 3rd difference - remove all minimum elements
                for(ArrayList<Tuple> tuple : tuples){
                    if(!tuple.isEmpty()) {
                        if(tuple.get(0).getField(fieldName).type.equals(minTuple.getField(fieldName).type)){
                            if(tuple.get(0).getField(fieldName).type.equals(FieldType.STR20)){
                                if(tuple.get(0).getField(fieldName).str.equals(minTuple.getField(fieldName).str))
                                    tuple.remove(0);
                            }else{
                                if(tuple.get(0).getField(fieldName).integer == minTuple.getField(fieldName).integer)
                                    tuple.remove(0);
                            }
                        }
                    }
                }
            }
        }

        //for(Tuple tuple : res) System.out.println(tuple);
        clearMainMemory();
        return res;
    }

    public void selectQueryForMultiTable(ParseTreeNode parseTree) {
        Relation relation = null;
        ArrayList<String> tempRelations = new ArrayList<>();
        ArrayList<Tuple> selected = new ArrayList<>();
        ArrayList<String> selectedAttr = new ArrayList<>();
        if(parseTree.tableList.size()==2) {
            if(parseTree.expressionTree != null && parseTree.expressionTree.natureJoin.size() > 0){
                Pair<ArrayList<String>, String> condition = parseTree.expressionTree.natureJoin.get(0);
                ArrayList<String> joinInfo = multipleSelectHelper(condition);
                relation = Join.naturalJoin(schemaManager, mainMemory, joinInfo.get(0), joinInfo.get(1), joinInfo.get(2));
            }else {
                relation = Join.crossProduct(schemaManager, mainMemory, parseTree.tableList.get(0), parseTree.tableList.get(1));
            }
        }else {
            if(parseTree.expressionTree != null && parseTree.expressionTree.natureJoin.size() !=0){
                for(int i=0; i<2; i++){
                    Pair<ArrayList<String>, String> condition = parseTree.expressionTree.natureJoin.get(i);
                    ArrayList<String> joinInfo = multipleSelectHelper(condition);
                    if(i==0) {
                        relation = Join.naturalJoin(schemaManager, mainMemory, joinInfo.get(0), joinInfo.get(1), joinInfo.get(2));
                        // break;
                    }else{

                        relation = Join.crossProduct(schemaManager, mainMemory, relation.getRelationName(), joinInfo.get(1));
                    }
                }
            }else{
                for(int i=0; i<parseTree.tableList.size()-1; i++){
                    if(i==0){
                        relation = Join.crossProduct(schemaManager, mainMemory, parseTree.tableList.get(0), parseTree.tableList.get(1));
                    }else{
                        relation = Join.crossProduct(schemaManager, mainMemory, relation.getRelationName(), parseTree.tableList.get(i+1));
                    }
                }
            }
        }
        tempRelations.add(relation.getRelationName());


        //if DISTINCT
        if(parseTree.distinct){
            relation = distinct(schemaManager, relation, mainMemory, parseTree.selectList);
            clearMainMemory();
            tempRelations.add(relation.getRelationName());
        }

        //selection
        if(parseTree.where){
            relation = select(schemaManager, relation, mainMemory, parseTree);
            clearMainMemory();
            tempRelations.add(relation.getRelationName());
        }

        //if ORDER BY
        if(parseTree.order){
            relation = sort(schemaManager, relation, mainMemory, parseTree.orderBy);
            clearMainMemory();
            tempRelations.add(relation.getRelationName());
        }

        //projection
        //System.out.println(relation.getNumOfTuples());
        project(relation, mainMemory, parseTree, selected, selectedAttr);

        //System.out.println(relation.getRelationName());
        ArrayList<String> names = relation.getSchema().getFieldNames();
//        for(String name:names){
//            System.out.println(name);
//        }
        if(tempRelations.isEmpty()) return;
        for(String temp : tempRelations){
            if(schemaManager.relationExists(temp)) schemaManager.deleteRelation(temp);
        }
    }
    private static ArrayList<String> multipleSelectHelper(Pair<ArrayList<String>, String> condition){
        ArrayList<String>joinInfo = new ArrayList<String>();
        joinInfo.add(condition.first.get(0));
        joinInfo.add(condition.first.get(1));
        joinInfo.add(condition.second);
        return joinInfo;
    }

    public static void main(String[] args) {
        PhysicalQuery query = new PhysicalQuery();

        query.execute("CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)");

        query.execute("INSERT INTO course (sid, grade, exam, project, homework) VALUES (1, \"E\", 100, 100, 100)");
        query.execute("INSERT INTO course (sid, grade, exam, project, homework) VALUES (1, \"E\", 100, 100, 100)");
        query.execute("INSERT INTO course (sid, grade, exam, project, homework) VALUES (1, \"E\", 100, 100, 99)");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 100, 98, \"C\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 50, 90, \"E\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 100, 100, \"A\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (9, 100, 100, 97, \"A\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (6, 50, 50, 61, \"D\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (7, 0, 0, 0, \"E\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (8, 0, 0, 0, \"E\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (5, 50, 50, 59, \"D\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (10, 50, 50, 56, \"D\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (11, 0, 0, 0, \"E\")");
//        query.execute("INSERT INTO course (sid, homework, project, exam, grade) VALUES (12, 100, 100, 66, \"A\")");
        query.execute("INSERT INTO course (sid, homework, project, exam, grade) SELECT * FROM course");
        //query.execute("SELECT * FROM course");
        query.execute("CREATE TABLE course2 (sid INT, exam INT, grade STR20)");
        query.execute("INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, \"A\")");
        query.execute("INSERT INTO course2 (sid, exam, grade) VALUES (2, 100, \"A\")");
        query.execute("INSERT INTO course2 (sid, exam, grade) VALUES (3, 100, \"A\")");
        query.execute("INSERT INTO course2 (sid, exam, grade) VALUES (4, 100, \"A\")");
        //query.execute("SELECT course.sid, course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid");
        //query.execute("SELECT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid");
        query.execute("SELECT DISTINCT course.grade, course2.grade FROM course, course2 WHERE course.sid = course2.sid AND [ course.exam > course2.exam OR course.grade = \"A\" AND course2.grade = \"A\" ] ORDER BY course.exam");

        //query.execute("DROP TABLE course");
//        query.execute("SELECT * FROM course");
//        SELECT * FROM course ORDER BY exam
//        SELECT * FROM course WHERE exam = 100
//        SELECT * FROM course WHERE grade = "A"
//        SELECT * FROM course WHERE exam = 100 AND project = 100
//        SELECT * FROM course WHERE exam = 100 OR exam = 99
//        SELECT * FROM course WHERE NOT exam = 0
//        SELECT * FROM course WHERE exam > 70
//        SELECT * FROM course WHERE exam = 100 OR homework = 100 AND project = 100
//        SELECT * FROM course WHERE [ exam = 100 OR homework = 100 ] AND project = 100
//        SELECT * FROM course WHERE exam + homework = 200
//        SELECT * FROM course WHERE ( exam * 30 + homework * 20 + project * 50 ) / 100 = 100
//        SELECT * FROM course WHERE grade = "C" AND [ exam > 70 OR project > 70 ] AND NOT ( exam * 30 + homework * 20 + project * 50 ) / 100 < 60
//        System.out.println();
//        //query.execute("SELECT sid, grade FROM course WHERE sid < 5 ORDER BY grade");
//        query.execute("CREATE TABLE course2 (sid INT, exam INT, grade STR20)");
//        query.execute("INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, \"A\")");
//        query.execute("INSERT INTO course2 (sid, exam, grade) VALUES (1, 101, \"A\")");
//        query.execute("SELECT * FROM course, course2");
    }
}














