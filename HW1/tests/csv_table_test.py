from src.CSVDataTable import CSVDataTable

import json

def test_create_initial():

    print("\nThis is a test for initialiazation of CSVTable")
    t1 = CSVDataTable("People",
                      connect_info={
                          "directory": "/Users/chenchenwei/Desktop/COMS4111 Databases/HW1/Data",
                          "file_name": "People.csv"
                      },
                      key_columns=['playerID'], debug=True)

    print("t1 = ", t1)


def test_find_function():

    print("\n This is a test for find_by_tmp function and find_by_primary_key of CSVTable")
    t1 = CSVDataTable("offices",
                      connect_info={
                          "directory": "/Users/chenchenwei/Desktop/COMS4111 Databases/HW1/Data",
                          "file_name": "offices.csv"
                      },
                      key_columns=['officeCode'], debug=True)
    print("t1 = " , t1)

    print("\n test find_by_template: 'city: Boston' ")
    print(t1.find_by_template(template={'city': 'Boston'}, field_list= ['officeCode', 'city', 'state']))

    print("\n test find_by_primary_key: '1'")
    print(t1.find_by_primary_key([ '1' ], field_list=None))

    print("\n test find_by_primary_key: 'zychto0111', should return None" )
    print(t1.find_by_primary_key(['zychto0111'], field_list=None) )


def test_delete():

    t = CSVDataTable("People",
                      connect_info={
                          "directory": "/Users/chenchenwei/Desktop/COMS4111 Databases/HW1/Data",
                          "file_name": "People.csv"
                      },
                      key_columns=['playerID'], debug=True)

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


def test_update_insert():
    t = CSVDataTable("People",
                      connect_info={
                          "directory": "/Users/chenchenwei/Desktop/COMS4111 Databases/HW1/Data",
                          "file_name": "People.csv"
                      },
                      key_columns=[ 'playerID' ], debug=True)

    new_person = {
        "playerID": "dff201",
        "nameLast": "w",
        "nameFirst": "c"
    }

    result = t.insert(new_person)
    print("result = ", json.dumps(result, indent=2))

    tmp = {"playerID": "dff201"}
    new_c = {
        "nameFirst": "DD",
        "birthMonth": "11"
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


test_create_initial()
test_find_function()
test_delete()
test_update_insert()
