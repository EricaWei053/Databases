from src.BaseDataTable import BaseDataTable

class DerivedDataTable(BaseDataTable):

    def __init__(self, table_name, rows):

        self._rows = rows
        super().__init__(table_name, None, None)

    def __str__(self):

        result = "DerivedTable name = " + self._table_name
        result += " \n Table type = < class DerivedDataTable > "
        s = 5

        if self._rows:
            no_print = min(s, len(self._rows))
            num_rows = len(self._rows)

        else:
            no_print = 0
            num_rows = 0

        result += "\n No. of rows = " + str(num_rows)
        result += "\n First five rows or only rows less than 5: "
        for i in range(no_print):
            result += '\n' + str(self._rows[i])

        return result


    def get_rows(self):
        return self._rows

    def find_by_primary_key(self, key_fields, field_list=None):
        """
        :param key_fields: The values for the key_columns, in order, to use to find a record. For example,
            for Appearances this could be ['willite01', 'BOS', '1960']
        :param field_list: A subset of the fields of the record to return. The CSV file or RDB table may have many
            additional columns, but the caller only requests this subset.
        :return: None, or a dictionary containing the columns/values for the row.
        """

        return NotImplemented()


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

        return super().find_by_template(template, field_list, limit, offset, order_by)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records. Raises an exception if this
            creates a duplicate primary key.
        :return: None
        """
        return NotImplemented()

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        return NotImplemented()

    def delete_by_key(self, key_fields):
        """

        Delete record with corresponding key.

        :param key_fields: List containing the values for the key columns
        :return: A count of the rows deleted.
        """
        return NotImplemented()

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        return NotImplemented()

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of values for primary key fields
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records. This returns an error if the update would create a duplicate primary key. NO ROWS are
            update on this error.
        :return: The number of rows updates.
        """
        return NotImplemented()


