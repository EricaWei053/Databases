from src.BaseDataTable import BaseDataTable
from src.DerivedDataTable import DerivedDataTable
import csv
import copy

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. will extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns=None, debug=True):
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
        self._connect_info = connect_info
        self._key_columns = key_columns
        self._debug = debug
        self._column_names = None
        self._rows = None
        self._load()

    def _add_rows(self, r):

        if self._rows is None:
            self._rows = []

        k = self._get_key(r)
        test_it = self.find_by_primary_key(k)
        if test_it is not None:
            raise Exception("Primary key duplicated")
        else:
            self._rows.append(r)

    def __str__(self):
        result = "CSVDataTable: name = " + self._table_name + ", \nconnect_info = " + \
            str(self._connect_info) + ",\n key_columns= " + str(self._key_columns)

        if self._rows is not None:
            result += "\n No. of rows = " + str(len(self._rows))
            result += '\n Column Names: ' + str(self._column_names)

        else:
            result += "No rows "

        to_print = min(len(self._rows), 5)
        result += "\n First " + str(to_print) + " rows to display: "
        for i in range(to_print):
            result += "\n " + str(dict(self._rows[i]))

        return result

    def _load(self):

        path = self._connect_info["directory"]
        file_name = self._connect_info["file_name"]

        self._rows = []
        with open(path + "/" + file_name, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            for row in csv_reader:
                if self._column_names is None:
                    self._column_names = list(row.keys())
                self._rows.append(dict(row))

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record. For example,
            for Appearances this could be ['willite01', 'BOS', '1960']
        :param field_list: A subset of the fields of the record to return. The CSV file or RDB table may have many
            additional columns, but the caller only requests this subset.
        :return: None, or a dictionary containing the columns/values for the row.
        """

        tmp = dict(zip(self._key_columns, key_fields))
        result = self.find_by_template(tmp, field_list)
        rows = result.get_rows()

        if rows and len(rows) > 0:
            return rows[0]
        else:
            return None

    def _matches_tempalte(self, tmp, row):
        if tmp is None:
            return True

        keys = tmp.keys()
        for k in keys:
            v = row.get(k)

            if tmp[k] != v:
                return False
        return True

    def _project(self, field_list, rows):

        result = []
        if field_list is None:
            result = rows

        else:
            for r in rows:
                new_r = {f: r[ f ] for f in field_list}
                result.append(new_r)

        return result


    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}. The function will return
            a derived table containing the rows that match the template.
        :param field_list: A list of requested fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A derived table containing the computed rows.
        """

        result = None

        for row in self._rows:
            if self._matches_tempalte(template, row):
                if result is None:
                    result = []
                result.append(copy.copy(row))

        new_result = self._project(field_list, result)

        return DerivedDataTable("FBT: " + self._table_name, new_result)

    def _get_key(self, row):
        result = [row[k] for k in self._key_columns]
        return result

    def insert(self, new_record):
        """
        :param new_record: A dictionary representing a row to add to the set of records. Raises an exception if this
            creates a duplicate primary key.
        :return: None
        """
        self._add_rows(new_record)

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """

        new_rows = []
        count = 0

        for row in self._rows:
            if not self._matches_tempalte(template, row):
                new_rows.append(copy.copy(row))
            else:
                count += 1

        self._rows = new_rows

        return count

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """

        tmp = dict(zip(self._key_columns, key_fields))
        return self.delete_by_template(tmp)


    def _update_row(self, r, new_value):
        keys = new_value.keys()
        new_r = copy.copy(r)

        for k in keys:
            new_r[k] = new_value[k]

        return new_r

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        count = 0

        for row in self._rows:
            if self._matches_tempalte(template, row):
                count += 1
                new_r = self._update_row(row, new_values)
                new_k = self._get_key(new_r)

                k = self.find_by_primary_key(new_k)
                if k is None:
                    self._rows.remove(row)
                    self._add_rows(new_r)
                else:
                    raise ValueError("Duplicated primary Key")

        return count


    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """

        template = dict(zip(self._key_columns, key_fields))

        return self.update_by_template(template, new_values)
