import sqlparse
import sys
import traceback
import copy
import csv
import operator


ops = {
    "<": operator.lt,
    ">": operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "=": operator.eq
}
def readMetaData(dictionary):
    try:
        f = open('./metadata.txt', 'r')
        checkIfNewTable = 0
        for line in f:
            line = line.strip()
            if line == "<begin_table>":
                checkIfNewTable = 1
                continue
            if checkIfNewTable == 1:
                tableName = line
                dictionary[tableName] = []                          
                checkIfNewTable = 0
                continue
            if not line == '<end_table>':
                dictionary[tableName].append(line)

    except: 
        print "Cannot read metadata.txt"
        sys.exit(1)


def checkIfcolumnIsPresent(column_val,tables):
    try:
        if '.' in column_val:
            colx = column_val.split('.')
            f1 = colx[1] in dictionary[colx[0]]
            f2 = colx[0] in tables
            if not f1 or not f2:
            	print "Column is Invalid"
            	sys.exit(1)
            return column_val
        for t in tables:
            if column_val in dictionary[t]:
                return t + '.' + column_val
        print "Column is Invalid"
        sys.exit(1)
    except:
        print "Exception occured while checking if column is present"
        sys.exit(1)

def getType(token):
    try:
        return type(token).__name__
    except:
        print "Type cannot be computed"
        sys.exit(1)

def join(tables):
    try:
        join_tb = []
        table = tables[0]
        dictionary_join = {}
        if len(tables) == 1:
            for row in alltables[table]:
                dictionary_join = {}
                for colum in row.keys():
                    var = table + '.' + colum
                    dictionary_join[var] = row[colum]
                join_tb.append(dictionary_join)
        else:
            t2 = tables[1:]
            other_tb = join(t2)
            for i in range(len(alltables[table])):
                row1 = alltables[table][i]
                for row2 in other_tb:
                    dictionary_join = copy.deepcopy(row2)
                    for colum in row1.keys():
                        var = table + '.' + colum
                        dictionary_join[var] = row1[colum]
                    join_tb.append(dictionary_join)
        return join_tb
    except:
        print "Exception occured in joining tables"
        sys.exit(1)

def apply(x,y,operator):
    try:
        return ops[operator](int(x),int(y))
    except:
        print "Given operator is not implemented"
        sys.exit(1)

def checkForSelect(first_term):
    if str(first_term).lower() != "select":
        print("Only select query is supported")
        sys.exit(1)

def checkForDistinct(query_term):
    if str(query_term).lower() == "distinct":
        return 1,4
    return 0,2

def checkForValidTable(tables):
    for table_name in tables:
        if table_name not in dictionary:
            print("Table name does not exist")
            sys.exit(-1)

def getTablesFromQuery(query_term):
    var = type(query_term).__name__
    tables = []
    if var == 'Identifier':
        tables = [str(query_term)]
    else: 
        tables = list(query_term.get_identifiers())
        tables = [ str(x) for x in tables]
    return tables

def getColumns(token, tables):
    colsaggfn = []
    colsaggcol = []
    column_names = []
    if str(token.ttype) == "Token.Wildcard":
        for tb in tables:
            for col in dictionary[tb]:
                column_names.append(tb + '.' + col )
    else:
        if getType(token) != 'IdentifierList':
            column_names = [token]
        else:
            column_names = list(token.get_identifiers())

        tempcol = []

        for column in column_names:
            if getType(column) == 'Function':
                for token in column.tokens:
                    var = getType(token)
                    if var == 'Parenthesis':
                        column.tokens += token.tokens[1:-1]
                    elif var == 'Identifier':
                        if len(colsaggfn) == 0:
                            colsaggfn.append(str(token).lower())
                        else:
                            colsaggcol.append(checkIfcolumnIsPresent(str(token),tables))
                            break
                tempcol.append(colsaggfn[-1] + '(' + colsaggcol[-1] + ')')
            else:
                tempcol.append(str(column))
        column_names = tempcol

    return colsaggcol,colsaggfn,column_names 

def getNewJoinWhere(joining_tables, where, tables, column_names):
    new_join = []
    for i,row in enumerate(joining_tables):
        isand = 1
        ans = True
        for j,condition in enumerate(where):     
            operator = None
            isReversed = 0
            stat = None
            typeOf = str(condition.ttype)
            val = str(condition).lower()
            L = []
            typeofCondition = getType(condition)
            if typeOf == "Token.Keyword" and val == "or":
                isand = 0
            elif typeOf == "Token.Keyword" and val == "and":
                isand = 1
            elif typeofCondition == 'Comparison':
                for token in condition.tokens:
                    typeofToken = getType(token)
                    if token.ttype is not None:
                        typeofvar = str(token.ttype)
                        if typeofvar == 'Token.Operator.Comparison':
                            operator = str(token)
                        elif typeofvar.startswith('Token.Literal'):
                            L.append(int(str(token)))
                        
                    elif typeofToken == 'Identifier':
                        col_cur = checkIfcolumnIsPresent(str(token),tables)
                        L.append(row[col_cur])
                       
                if (len(L)<2):
                    stat = 1
                else:
                    stat = apply(L[0], L[1], operator)
            if not stat is None:
                if isand == 1:
                    ans = ans and stat
                else:
                    ans = ans or stat
        if ans == 1:
            new_join.append(row)

    return new_join


def Query(command):
    colsaggfn = []
    colsaggcol = []
    query = sqlparse.parse(command)[0]
      #Check if it is select query or not
    checkForSelect(query.tokens[0])
    distinct, ind  = checkForDistinct(query.tokens[2])

    #Extract tables depending on count 
    tables = getTablesFromQuery(query.tokens[ind+4])

    #Check if table names given are valid using metadata
    checkForValidTable(tables)
    
    #Filling tables and handling columns
    colsaggcol, colsaggfn, column_names = getColumns(query.tokens[ind], tables)

   
    for i in range(len(column_names)):
        if '(' not in column_names[i]:
            column_names[i] = checkIfcolumnIsPresent(column_names[i],tables)

    joining_tables = join(tables)

    where = []
    if len(query.tokens) <= ind+6:
        where = []
    else:
        where = query.tokens[ind+6].tokens
        where = where[2:]

    if len(where) > 0:
        joining_tables = getNewJoinWhere(joining_tables,where, tables, column_names)
       

    if distinct == 1:
        new_join = []
        st = set()
        for row in joining_tables:
            temp = str([row[col] for col in column_names])
            if temp in st:
                continue
            else:
                st.add(temp)
                new_join.append(row)
        joining_tables = new_join


    if len(joining_tables) > 0 and len(colsaggfn) > 0:
        for i, fn in enumerate(colsaggfn):
            col = colsaggcol[i]
            values = joining_tables[0][col]
            for row in joining_tables:
                if str(fn).lower() == 'max':
                    values = max(values, row[col])
                elif str(fn).lower() == 'sum':
                    values = values + row[col]
                elif str(fn).lower()== 'avg':
                    values = values + row[col]
                elif str(fn).lower() == 'min':
                    values = min(values, row[col])
                else:
                    print 'Given Function is not implemented'
                    sys.exit(1)
            if str(fn).lower() == 'avg':
                values = round(float(values) / len(joining_tables), 2)
            for row in joining_tables:
                fname = '{}({})'.format(fn, col)
                row[fname] = values
        if len(joining_tables) > 1:
            joining_tables = [joining_tables[0]] 

    writer = csv.DictWriter(sys.stdout, column_names, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(joining_tables)

def main():
    
    readMetaData(dictionary)
    for tableName in dictionary:
        tname = tableName + '.csv'
        val=[];
        with open(tname, 'rb') as f:
            csvr = csv.reader(f)
            for row in csvr:
                r = {}
                columns = len(row)
                for i in range(columns):
                    col_name = dictionary[tableName][i]
                    r[col_name] = int(row[i])
                val.append(r)
        alltables[tableName] = val
    try:
        Query(sys.argv[1])
    except Exception:
        traceback.print_exc()

dictionary = {}
alltables = {}
main()
