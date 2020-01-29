from src.CSVDataTable import CSVDataTable
from src.RDBDataTable import RDBDataTable
import json

def test1():
    t = RDBDataTable("People" , ["playerID"])
    print(t)

def test2():
    t = RDBDataTable("People" , ["playerID"], debug=True)
    tmp = {"nameLast": "Williams"}
    result = t._template_to_where_clause(tmp)
    print("WC = ", str(result))

    q = "select * from People " + result[0]

    result1 = t._run_q(cnx = None, q=q, args = result[1], commit=True, fetch=True)
    print("Query result = ", json.dumps(result1, indent=2, default=str))

    print(q)

def test3():
    t = RDBDataTable("People" , ["playerID"])
    tmp = {"nameLast": "Williams"}
    result = t.find_by_template(tmp)

    print("Query result = ", result)


def test4():
    t = RDBDataTable("People" , ["playerID"])
    #tmp = {"nameLast": "Williams"}
    result = t.find_by_primary_key(["willite01"], field_list=["playerID", "nameLast", "nameFirst"])

    print("result = ", result)


def test5():
    t = RDBDataTable("People", key_columns=None, debug=True)
    result = t._get_primary_key()

    print("result = ", json.dumps(result,  indent=2) )



def test6():
    t = RDBDataTable("People", key_columns=None, debug=True)
    new_person = {
        "playerID" : "dff201",
        "nameLast" : "w",
        "nameFirst" : "c"
    }
    result = t.insert(new_person)

    print("result = ", json.dumps(result,  indent=2))

def test7():
    t = RDBDataTable("People", key_columns=None, debug=True)
    new_person = {
        "playerID" : "dff201",
        "nameLast" : "w",
        "nameFirst" : "c"
    }
    result = t.insert(new_person)

    print("result = ", json.dumps(result,  indent=2) )
    tmp = {"playerID": "dff201"}
    r1 = t.find_by_template(tmp)
    print("After insert", r1)
    r2 = t.delete_by_template(tmp)

    print("After delete ", r2 )


def test8():
        t = RDBDataTable("People", key_columns=["playerID"], debug=True)
        new_person = {
            "playerID": "dff201",
            "nameLast": "w",
            "nameFirst": "c"
        }

        result = t.insert(new_person)
        print("result = ", json.dumps(result, indent=2))


        tmp = {"playerID": "dff201"}
        new_c = {
            "nameFirst" : "DD",
            "birthMonth" : "11"
        }

        r1 = t.find_by_template(tmp)
        print("After insert", r1)

        r2 = t.update_by_template(tmp, new_c)
        print("After update", r2)
        r3 = t.find_by_template(tmp)
        print("Find", r3)

        r4 = t.delete_by_template(tmp)
        print("After delete ", r4)
        r1 = t.find_by_template(tmp)
        print("Find again", r1)

#test1()
test2()
test3()
test4()
test5()
test6()
test7()

test8()