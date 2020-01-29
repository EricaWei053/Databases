import pymysql
import json
import csv
from operator import  itemgetter


path = "/Users/chenchenwei/Desktop/COMS4111 Databases/HW1/Data"

_default_connect_info = {
            'host': 'localhost',
            'user': 'root',
            'password': 'WCCxb960503',
            'db': 'lahman2017',
            'port': 3306
        }


_cnx = pymysql.connect(
                host=_default_connect_info['host'],
                user=_default_connect_info['user'],
                password=_default_connect_info['password'],
                db=_default_connect_info['db'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)


def _run_q(q, args=None, fields=None, fetch=True, cnx=None, commit=True):
        """

        :param q: An SQL query string that may have %s slots for argument insertion. The string
            may also have {} after select for columns to choose.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :param commit: Do not worry about this for now. This is more wizard stuff.
        :return: A result set or None.
        """

        # Use the connection in the object if no connection provided.
        if cnx is None:
            cnx = _cnx

        # Convert the list of columns into the form "col1, col2, ..." for following SELECT.
        if fields:
            q = q.format(",".join(fields))

        cursor = cnx.cursor()  # Just ignore this for now.

        # If debugging is turned on, will print the query sent to the database.
        print("Query = ", cursor.mogrify(q, args))

        cnt = cursor.execute(q, args)  # Execute the query.

        # Technically, INSERT, UPDATE and DELETE do not return results.
        # Sometimes the connector libraries return the number of created/deleted rows.
        if fetch:
            r = cursor.fetchall()  # Return all elements of the result.
        else:
            r = cnt

        if commit:  # Do not worry about this for now.
            cnx.commit()

        return r


def load(fn):
    file_name = fn
    result = []
    with open(path + "/" + file_name, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for r in csv_reader:
            result.append(r)

    return result


people = load("People.csv")
print("No of people = ", len(people))
batting = load("Batting.csv")
print("No of batting = ", len(batting))
appearances = load("Appearances.csv")
print("No of appearances = ", len(appearances))


def compute_eligible_people():
    result = {}

    for a in appearances:

        if a["yearID"] >= '1960':
            p = result.get(a['playerID'], None)
            if p is None:
                result[a['playerID']] = "cat"

    return list(result.keys())


eligible_players = compute_eligible_people()
print("No of eligible people = ", len(eligible_players))


def compute_career_stats(p):
    total_h = 0
    total_abs = 0

    for b in batting:
        if b['playerID'] == p:
            total_h += int(b['H'])
            total_abs += int(b["AB"])

    avg = 0
    if total_abs > 0:
        avg = total_h / float(total_abs)

    return [p, avg, total_h, total_abs]


def compute_all_totals():

    result = []
    count = 0

    for p in eligible_players:
        bat = compute_career_stats(p)
        count += 1
        if count % 100 == 0:
            print("Did eligible ", count, " current = ", bat)
        if bat[3] > 200:
            result.append(bat)

    return result


def add_names(final_result):

    for f in final_result:
        pid = f[0]
        for p in people:
            if p['playerID'] == pid:
                f.append(p['nameLast'])
                f.append(p['nameFirst'])


def main():
    q = "SELECT Batting.playerID, \
        (SELECT People.nameFirst FROM People WHERE People.playerID=Batting.playerID) as first_name,\
        (SELECT People.nameLast FROM People WHERE People.playerID=Batting.playerID) as last_name, \
        sum(Batting.h)/sum(batting.ab) as career_average, \
        sum(Batting.h) as career_hits, \
        sum(Batting.ab) as career_at_bats,\
        min(Batting.yearID) as first_year, \
        max(Batting.yearID) as last_year \
        FROM \
        Batting \
        GROUP BY \
        playerId \
        HAVING \
        career_at_bats > 200 AND last_year >= 1960 \
        ORDER BY \
        career_average DESC \
        LIMIT 10; "

    result1 = _run_q(q)
    print("SQL Result = ", json.dumps(result1, indent=2))


    final_result = compute_all_totals()
    final_result = sorted(final_result, reverse=True, key=itemgetter(1))
    final_result = final_result[0:10]
    add_names(final_result)

    print("CSV Result = ", json.dumps(final_result, indent=2))
    print("\n")


main()