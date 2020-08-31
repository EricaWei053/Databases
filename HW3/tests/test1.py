#Chenchen Wei - cw3137

from src import CSVDataTable
import csv
import json
import logging
import time

logging.basicConfig(level=logging.INFO)


def load(fn):

    result = []
    cols = None
    with open(fn, "r") as infile:
        rdr = csv.DictReader(infile)
        cols = rdr.fieldnames
        for r in rdr:
            result.append(r)

    return result, cols


def t1():
    """
    Test import_data and find_by_template functions.
    :return:
    """
    print("Test import_data(which including add_rows(), and test find_by_tmp not use index. ")

    rows, cols = load("../CSVFile/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    tmp = {'playerID': 'willite01'}

    start = time.time()
    for i in range(0, 1000):
        r = t.find_by_template(tmp, fields=None, use_index=False).get_rows()
        if i == 0:
            print("Row = ", r)

    end = time.time()
    elapsed = end - start
    print("Time : ", elapsed)

    t.save()


def t2():
    """
    Test add_index function, use index.
    :return:
    """

    print("Test add_index function, find_by_template use index.")

    rows, cols = load("../CSVFile/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    tmp = {'playerID': 'willite01', "nameLast": "Williams"}
    t.add_index("LASTNAME", ['nameLast', 'nameFirst'], "INDEX")

    start = time.time()

    for i in range(0, 1000):
        r = t.find_by_template(tmp, fields=None, use_index=True).get_rows()
        if i == 0:
            print("Row = ", r)

    end = time.time()
    elapsed = end - start
    print("Time : ", elapsed)



def t3():

    """
    Test compute_index_value function.
    :return:
    """
    print("Test compute_index_value function.")
    rows, cols = load("../CSVFile/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols, primary_key_columns=['uni'])
    t.import_data(rows)
    print("T = ", t)

    i = CSVDataTable.Index(name="Bob", table= t, columns=['last_name', 'first_name'], kind="UNIQUE")
    r = {"last_name": "Wei", "first_name": "Erica", "uni":"cw1177"}
    kv = i.compute_index_value(r)
    print(" KV = ", kv)
    i.add_to_index(row=r, rid="3")
    print(i)
    #print("Should return error here. Duplicate"), correct
    #i.add_to_index(row=r, rid="4")

    t.save()


def t4():
    """
    Test join function.
    :return:
    """

    print("test JOIN function. ")

    rows, cols = load("../CSVFile/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    rows, cols = load("../CSVFile/BattingSmall.csv")
    t2 = CSVDataTable.CSVDataTable(table_name="BattingSmall", column_names=cols, primary_key_columns=['playerID', 'teamID', 'yearID', 'stint'])
    t2.import_data(rows)
    print("T = ", t)

    j = t2.join(t, ['playerID'],
                w_clause={"People.nameLast": "Williams", "People.birthCity": "San Diego"},
                p_clause=['playerID', "People.nameLast", "People.nameFirst", "BattingSmall.teamID",
                          "BattingSmall.yearID", "BattingSmall.stint", "BattingSmall.H", "BattingSmall.AB"], optimize=True)

    print("Result = ", j)
    print(j.get_rows())
    print("All rows = ", json.dumps(j.get_rows(), indent=2))


def t5():
    """
    Test get_specific_project function
    :return:
    """
    print("Test get_specific_project function.")

    rows, cols = load("../CSVFile/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    p = ["playerID", "People.nameLast", "Batting.H"]
    print("Specific template = ", t._get_specific_project(p))


def t6():
    """
    Test add_index function
    :return:
    """

    print("Test add_index function in csvDataTable. ")
    rows, cols = load("../CSVFile/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)
    t.add_index("LASTNAME", ['nameLast'], "INDEX")
    tmp = {"nameLast": "Williams", "birthCity": "San Diego"}

    r = t.find_by_template_rows(tmp, fields=None, use_index=True)
    print("Answer = ", r)

def t7():
    """
    Test compute_index_value function.
    :return:
    """
    print("Test compute_index_value function. Test delete_from_index  ")

    rows, cols = load("../CSVFile/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="rings", column_names=cols, primary_key_columns=['uni'])
    t.import_data(rows)

    print("T = ", t)

    t.add_index("name", ["last_name", "first_name"], "INDEX")
    tmp = {"last_name": "Baggins", "first_name": "Bilbo"}
    r = t.find_by_template(tmp, fields=None, use_index=True)
    idx = t._indexes['name']
    r2 = r._rows[2]
    k = idx.compute_index_value(r2)
    print("Index key is ", k)

    #row 2's rid is 3. index is less one than rid.
    idx.delete_from_index(r2, 3)


def t8():

    print("Test insert() function. ")

    rows, cols = load("../CSVFile/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="Rings", column_names=cols, primary_key_columns=['uni'])
    t.import_data(rows)
    t.save()
    print("Before insert T = ", t)
    new_row = {"last_name": "LoL", "first_name": "New", "uni": "LN11"}
    t.insert(new_row)
    print("After inset()\n T = ", t)
    print("Find the inseted row:")
    tmp = {"last_name": "LoL", "first_name": "New"}
    r = t.find_by_template(tmp, fields=None, use_index=True).get_rows()
    print(r)


def t9():

    "Test delete function. Check if row id is correct after deletion. "
    rows, cols = load("../CSVFile/rings.csv")
    t = CSVDataTable.CSVDataTable(table_name="Rings", column_names=cols, primary_key_columns=['uni'])
    t.import_data(rows)
    r = t.delete({"last_name": "Baggins"})
    print("delete:", r)
    print("\n\nafter delete, T = ", t)


def t10():

    "Test the time oj JOIN with and without optimization!! "

    rows, cols = load("../CSVFile/People.csv")
    t = CSVDataTable.CSVDataTable(table_name="People", column_names=cols, primary_key_columns=['playerID'])
    t.import_data(rows)
    print("T = ", t)

    rows, cols = load("../CSVFile/BattingSmall.csv")
    t2 = CSVDataTable.CSVDataTable(table_name="BattingSmall", column_names=cols, primary_key_columns=['playerID', 'teamID', 'yearID', 'stint'])
    t2.import_data(rows)
    print("T = ", t)

    print("\n\n With optimization\n")
    start_time = time.time()
    for i in range(0,1):
        j = t2.join(t, ['playerID'],
                    w_clause={"People.nameLast": "Williams", "People.birthCity": "San Diego"},
                    p_clause=['playerID', "People.nameLast", "People.nameFirst", "BattingSmall.teamID",
                              "BattingSmall.yearID", "BattingSmall.stint", "BattingSmall.H", "BattingSmall.AB"],
                    optimize=True)
    end_time = time.time()
    print("Elapsed time = ", end_time-start_time)

    print("\n\n Without optimization\n")
    start_time = time.time()
    for i in range(0, 1):
        j = t2.join(t, ['playerID'],
                    w_clause={"People.nameLast": "Williams", "People.birthCity": "San Diego"},
                    p_clause=['playerID', "People.nameLast", "People.nameFirst", "BattingSmall.teamID",
                              "BattingSmall.yearID", "BattingSmall.stint", "BattingSmall.H", "BattingSmall.AB"],
                    optimize=False)
    end_time = time.time()
    print("Elapsed time = ", end_time - start_time)


t1()
t2()
t3()
t4()
t5()
t6()
t7()
t8()
t9()
t10()

