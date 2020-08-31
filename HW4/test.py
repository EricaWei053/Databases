
import sys
sys.path.append(
    '/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/W4111New/w4111-Databases/HW_Solutions/HW4')

import social_graph.fan_comment as fc
import json
import pandas as pd

pd.set_option('display.width', 132)

fg = fc.FanGraph(
                  host="localhost",
                  port=7687,
                  secure=False)

print("fg = ", fg)


def t1(name):
    """
    Find all the movies that a person with name 'name' is related to.
    Return information as a Pandas data frame.
    """

    # The match is (Person)-[any relationship]->(Movie)
    # Person name must be the passed {name}.
    # Return a list of dictionaries of the attributes/values.
    #
    qs = "match (t:Person)-[r]->(t2:Movie) " + \
         " where t.name={name} " + \
         " return t.name, t2.title, t2.released, r.roles"

    # Set the name parameter for the query
    ag = {"name": name}

    # Use the handy, dandy run_q method on FanGraph
    r = fg.run_q(qs, ag)

    # Convert the result into a DataFrame
    x = pd.DataFrame(r.data())

    # Make sure the columns are in the "right" order.
    # Python dicts are unordered, which means the data frame's columns
    # might be in a weird order.
    x = x[['t.name', 't2.title', 't2.released', 'r.roles']]
    return x

t1()

def test_create_fan():
    r = fg.create_fan(uni="dff99999", last_name="Vader", first_name="Donald")
    return r

f = test_create_fan()
print("Create returned ...", f)
print("The type is ... ", type(f))

