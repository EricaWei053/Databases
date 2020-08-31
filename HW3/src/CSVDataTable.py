#Chenchne Wei - cw3137

import csv
import copy
import logging
import json
import time

# Notes from OH:
# row_id in dictionary
# row_id as index{k:v}
# {columns ： "willim_"}
# save entire thing  using row_id is
# index selectivity define template. go through index does this implement support index?
# csvtable -> keep rows in list, every entry index is the row_id
# problem delete a row? --> keep a counter --> dictionary of rowid and real row.
#
# data structure: dictionary of dictionaries
#
#

class Index():

    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, name=None, table=None, columns=None, kind=None, loadit=False):
        """
        Implements a hash index using a Python directory.
        :param name: Logical name of the index.
        :param table: Referene to the table for which this is an index.
        :param columns: The row columns that comprise the key, in the order they form the key.
        :param kind: One of the index_types.
        :param loadit: If evaluates to True, the index from the passed value.
        """

        if loadit:
            logging.debug("Loading an index.")
            self.from_json(table, loadit)
            logging.debug("Loaded index. name=%s, column=%s, kind=%s, table=%s",
                          self.name, str(self.columns), self.kind, self.table.get_table_name())
        else:
            logging.debug("Creating index. name=%s, column=%s, kind=%s, table=%s",
                          name, str(columns), kind, table.get_table_name())
            columns.sort()
            self.name = name
            self.kind = kind
            self.columns = columns
            self.table = table
            if table is not None:
                self.table_name = table.get_table_name()
            else:
                self.table_name = None
            self._index_data = None

        if self.columns is not None:
            self.columns.sort()

    def to_json (self):
        result = {}
        result["names"] = self.name
        result["columns"] = self.columns
        result["kind"] = self.kind
        result["table_name"] = self.table_name
        result["index_data"] = self._index_data

        return result

    def compute_index_value(self, row):

        terms = [row[k] for k in self.columns]
        result = "_".join(terms)
        return result

    def add_to_index(self, row, rid): #Not sure

        index_key = self.compute_index_value(row)

        if self._index_data is None:
            self._index_data = {}

        bucket = self._index_data.get(index_key, None)

        if self.kind != "INDEX" and bucket is not None:
            raise KeyError("Duplicate key: index = " + self.name + ", table= " + \
                           ", key = " + index_key)
        else:
            if bucket is None:
                bucket = []

            bucket.append(rid)
            self._index_data[index_key] = bucket

    def delete_from_index(self, row, rid):

        index_key = self.compute_index_value(row)
        bucket = self._index_data.get(index_key, None)
        if bucket is not None:
            bucket.remove(rid)

            if len(bucket) == 0:
                del (self._index_data[index_key])

    def _build(self):
        rows = self.table.get_rows_with_rids()

        self._index_data = {}
        for k,v in rows.items():
            self.add_to_index(v,k)

    def __str__(self):
        result = "Index name = " + self.name
        result += "\nTable names = " + self.table_name
        result += "\nColumns = " + str(self.columns)
        result += "\n kind = " + self.kind
        return result

    def from_json(self, table, loadit):
        self.name = loadit["name"]
        self.columns = loadit["columns"]
        self.kind = loadit["kind"]
        self.table = table
        self.table_name = loadit.get("table_name", table.get_table_name())
        self._index_data = loadit.get("index_data", None)

    def matches_index(self, template):
        """
        Determines if this index matches the query template.
        :param template: The query template
        :return:
            -None if the index does not match
            - The number of distinct entries in the index if it does match.
        """
        k = set(list(template.keys()))
        c = set(self.columns)

        if c.issubset(k):
            if self._index_data is not None:
                kk = len(self._index_data.keys())
            else:
                kk = 0
        else:
            kk = None

        return kk

    def find_rows(self, tmp):
        """
        Using the index, find the matching rows.
        :param tmp: Query template
        :return: Row Ids that match template.
        """
        t_keys = tmp.keys()
        t_vals = [tmp[k] for k in self.columns]
        t_s = "_".join(t_vals)

        #get corresponding index bucket
        d = self._index_data.get(t_s, None)

        return d

    def get_no_of_entries(self):
        return len(list(self._index_data.keys()))


class CSVDataTable():
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. will extend the
    base class and implement the abstract methods.
    """

    _default_directory = "../DB/"

    def __init__(self, table_name, column_names=None, primary_key_columns=None, loadit=False):
        """

        :param table_name: Name of the table. This is the table name for an RDB table or the file name for
            a CSV file holding data.
        :param connect_info: Dictionary of parameters necessary to connect to the data. See examples in subclasses.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
            A primary key is a set of columns whose values are unique and uniquely identify a row. For Appearances,
            the columns are ['playerID', 'teamID', 'yearID']
        :param debug: If true, print debug messages.
        """
        self._table_name = table_name
        self._primary_key_columns = primary_key_columns
        self._column_names = column_names

        self._indexes = None

        if not loadit:
            if column_names is None or table_name is None :
                raise ValueError("Did not provide table name or column names")

            self._next_row_id = 0
            self._rows = {}

            if primary_key_columns:
                self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")

    def __str__(self):
        """
        Produce a string/tex representation.
        :return:
        """
        result = "Table name =  " + self._table_name
        result += "\nColumn names = " + str(self._column_names)
        result += "\nPrimary key columns = " + str(self._primary_key_columns)
        result += "\nNext row id: " + str(self._next_row_id)

        if self._rows is not None:
            rids = self._rows.keys()
            rs = list(self._rows.values())
            num_rows = str(len(rids))

            result += "\nNo. of rows: " + num_rows

        return result

    def get_table_name(self):
        return self._table_name

    def add_index(self, index_name, column_list, kind):
        """
        Adds a new index and build the index data.
        :param index_name: Logical names of the index.
        :param column_list: List of columns for the index in order.
        :param kind: PRIMARY, UNIQUE, INDEX
        :return:
        """
        if index_name is None or column_list is None or kind is None:
            raise ValueError("Could not add index.")

        if self._indexes is None:
            self._indexes = {}

        idx = self._indexes.get(index_name, None)
        if idx is not None:
            raise ValueError("Duplicate index name.")
        cs = set(self._column_names)
        ks = set(column_list)
        if not ks.issubset(cs):
            raise ValueError("Key columns not subset of table columns.")

        index = Index(index_name, self, column_list, kind)
        self._indexes[index_name] = index

        self.build(index_name)

    def build(self, i_name):
        idx = self._indexes[i_name]
        for k,v in self._rows.items():
            idx.add_to_index(v, k)

    def drop_index(self, index_name):
        if index_name in self._indexes:
            del (self._indexes[index_name])
        else:
            raise ValueError("Delete: index_name is not in the dictionary.")

    def _get_primary_key(self, r):
        res = []
        for k in self._primary_key_columns:
            if r[k] is not None:
                res.append(r[k])
        return res

    def _get_primary_key_string(self, r):
        ks = self._get_primary_key(r)
        return str(ks)

    def _get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id

    def _add_row(self, r):

        if self._rows is None:
            self._rows = {}

        rid = self._get_next_row_id()
        self._rows[rid] = r

        if self._indexes is not None:
            for n, idx in self._indexes.items():
                idx.add_to_index(r, rid)

    def _remove_row(self, rid):

        r = self._rows[rid]
        for n, idx in self._indexes.items():
            idx.delete_from_index(r, rid)

        del[self._rows[rid]]
        return r

    def import_data(self, import_data):
        """
        Loads the data from CSV file into self._rows and self._index
        :param import_data
        :return:
        """
        for r in import_data:
            self._add_row(r)

    def save(self):
        d = {
            "sate" : {
                "table_name": self._table_name,
                "primary_key_columns": self._primary_key_columns,
                "next_rid": self._get_next_row_id(),
                "column_names": self._column_names
            }
        }

        fn = CSVDataTable._default_directory + self._table_name + ".json"
        d["rows"] = self._rows

        for k,v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string
            d['indexes'] = idxs

        d = json.dumps(d, indent =2)
        with open(fn, "w+") as outfile:
            outfile.write(d)

    def load(self):
        """
        You need to implement data load here.
        :return: None
        """
        fn = CSVDataTable._default_directory + self._table_name + ".json"

        with open(fn, "r") as infile:
            d = json.load(infile)

            state = d['state']
            self._table_name = state['table_name']
            self._primary_key_columns = state["primary_key_columns"]
            self._next_row_id = state['next_rid']
            self._column_names = state['column_names']
            self._rows = d['rows']

            for k,v in d['indexes'].items():
                idx = Index(loadit=v, table=self)
                if self._indexes is None:
                    self._indexes = {}
                self._indexes[k] = idx

                for k,b in d['indexes'].items():
                    idx = Index

    def get_rows_with_rids(self):
        return self._rows

    def get_rows(self):

        if self._rows is not None:
            result = []
            for k,v in self._rows.items():
                result.append(v)
        else:
            result = None
        return result

    def matches_template(self, row, tmp):

        result = False

        if tmp is None or tmp =={}:
            result = True
        else:
            for k in tmp.keys():
                v = row.get(k, None)
                if v!= tmp[k]:
                    result = False
                    break
            else:
                result = True

        return result

    def get_best_index(self, t): # completed
        """
        :param t: Template that we are using.
        :return: Most selective index.
        """

        best = None
        n = None

        #If I have indexes.
        if self._indexes is not None:

            #For every index name and index object.
            for k,v in self._indexes.items():

                #Determine if this index matches template
                cnt = v.matches_index(t)
                if cnt is not None:
                    if best is None:
                        best = cnt
                        n = k
                    else:
                        if cnt > best:
                            best = len(v.keys())
                            n=k
        return n

    def get_index_and_selectivity(self, cols):
        on_template = dict(zip(cols, [None] * len(cols)))
        best = None
        n = self.get_best_index(on_template)

        if n is not None:
            best = len(list(self._rows.keys()))/(self._indexes[n].get_no_of_entries())

        return n, best

    def find_by_index(self, tmp, idx):

        rids = idx.find_rows(tmp)
        res = {}
        for k,v in self._rows.items():
            if k in rids:
                res[k] = v

        return res

    def find_by_scan_template(self, tmp, rows):

        if tmp is None or tmp =={}:
            return rows
        result = {}
        for k, v in rows.items():
            if self.matches_template(v, tmp):
                result[k] = v

        return result

    def find_by_template(self, tmp, fields, use_index=True):
        """

        :param tmp:
        :param fields:
        :param use_index:
        :return:
        """
        if tmp is None:
            new_t = CSVDataTable(table_name="Derived: " + self._table_name, loadit=True)
            new_t.load_from_rows(table_name="Derived: " + self._table_name, rows=self.get_rows())
            return new_t

        result_rows = self.find_by_template_rows(tmp, fields, use_index)
        new_t = CSVDataTable(table_name="Derived: " + self._table_name, loadit=True)
        new_t.load_from_rows(table_name="Derived: " + self._table_name, rows=result_rows)

        return new_t

    def find_by_template_rows(self, tmp, fields=None, use_index=True):

        if tmp is None:
            return self._rows

        idx = self.get_best_index(tmp)
        logging.debug("Using index = %s", idx)

        if idx is None or use_index == False:
            result = self.find_by_scan_template(tmp, self.get_rows_with_rids())

        else:
            idx = self._indexes[idx]
            res = self.find_by_index(tmp, idx)
            result = self.find_by_scan_template(tmp, res)

        if result is not None:
            final_r = {}
            for k,v in result.items():
                if fields is not None:
                    final_new_r = {f: v[f] for f in fields}
                else:
                    final_new_r = v
                final_r[k] = final_new_r

            return final_r
        else:
            return None

    def insert(self, r):
        """
        :param new_record: A dictionary representing a row to add to the set of records. Raises an exception if this
            creates a duplicate primary key.
        :return: None
        """
        self._add_row(r)

    def delete(self, tmp):

        result = self.find_by_template_rows(tmp)
        rows = result
        deleted = []

        if rows is not None:
            print("check")
            for k in rows.keys():
                r = self._remove_row(k)
                deleted.append(r)

        return deleted

    def _get_sub_template(self, tmp, table_name):
        pass

    @staticmethod
    def _get_scan_probe(l_table, r_table, on_clause):
        s_best, s_selective = l_table.get_index_and_selectivity(on_clause)
        r_best, r_selective = r_table.get_index_and_selectivity(on_clause)

        result = l_table, r_table

        if s_best is None and r_best is None:
            result = l_table, r_table
        elif s_best is None and r_best is not None:
            result = r_table, l_table
        elif s_best is not None and r_best is None:
            result = l_table, r_table
        elif s_best is not None and r_best is not None and s_selective < r_selective:
            result = r_table, l_table

        return result

    def _get_specific_where(self, wc):
        result = {}
        if wc is not None:
            for k,v in wc.items():
                kk = k.split(".")
                if len(kk) == 1:
                    result[k] = v
                elif kk[0] == self._table_name:
                    result[kk[1]] = v

        if result == {}:
            result = None

        return result

    def _get_specific_project(self, p_clause):
        result = []
        if p_clause is not None:
            for k in p_clause:
                kk = k.split(".")
                if len(kk) == 1:
                    result.append(k)
                elif kk[0] == self._table_name:
                    result.append(kk[1])
        if result == []:
            result = None

        return result

    @staticmethod
    def on_clause_to_where(on_c, r):
        result = {c:r[c] for c in on_c}
        return result

    def load_from_rows(self, table_name, rows):

        self._table_name = table_name
        self._column_names = None
        self._indexes = None
        self._rows = {}
        self._next_row_id = 1

        for k,r in rows.items():
            r = dict(r)
            if self._column_names is None:
                self._column_names = list(sorted(r.keys()))
            self._add_row(r)

    def join(self, r_table, on_clause, w_clause, p_clause, optimize=True):

        if optimize:
            s_table, p_table = self._get_scan_probe(self, r_table, on_clause)

        else:
            s_table = self
            p_table = r_table

        if s_table != self and optimize:
            logging.debug("Swapping tables.")
        else:
            logging.debug("Not swapping tables.")

        logging.debug("Before pushdown, scan rows + %s", len(s_table.get_rows()))

        if optimize:
            s_tmp = s_table._get_specific_where(w_clause)
            s_proj = s_table._get_specific_project(p_clause)

            s_rows = s_table.find_by_template(s_tmp, s_proj, optimize)
            logging.debug("After pushdown, scan rows =%s", len(s_rows.get_rows()))

        else:
            s_rows = s_table

        scan_rows =s_rows.get_rows()
        result = {}

        cnt = 0
        start_time = time.time()

        for r in scan_rows:

            cnt += 1

            if cnt % 100 ==0:
                print("JOIN on", cnt, " iteration")
                elapsed_time = time.time() - start_time
                print("Time so far = ", elapsed_time)
                speed = cnt / elapsed_time
                print("Speed = ", speed, "rows per second ")

            p_where = CSVDataTable.on_clause_to_where(on_clause, r)
            p_project = p_table._get_specific_project(p_clause)

            p_rows = p_table.find_by_template(p_where, p_project, optimize)
            p_rows = p_rows.get_rows()

            count = 0
            if p_rows:
                for r2 in p_rows:
                    new_r = {**r, **r2}
                    result[count] = new_r
                    count += 1

        tn = "Join(" + self.get_table_name() + "," + r_table.get_table_name() + ")"
        final_result = CSVDataTable(
            table_name=tn,
            loadit=True
        )

        final_result.load_from_rows(table_name=tn, rows=result)

        ##Apply the result tmeplate table.

        return final_result