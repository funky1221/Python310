import sqlite3
import pandas as pd

class wsqlite:
    df_table = None
    conn = None
    tb_name = ''
    no_col = 0
    def __init__(self,
                 df: pd.DataFrame,     # dataframe
                 file: str,            # db file
                 tb_name: str,         # table name
                 pri_key: list=None ): # Primary key  ex: [header1,header2,....]

        self.df_table = df
        self.conn = sqlite3.connect(file)
        self.tb_name = tb_name
        self.create_table(pri_key)

    def __del__(self):
        self.conn.close()

    def __check_table(self) ->bool:
        c = self.conn.cursor()
        name = self.tb_name
        cursor = c.execute(f"""SELECT name FROM sqlite_master 
                           WHERE type='table' AND name='{name}';""")
        return True if cursor.fetchall() else False

    def __create_trigger(self):
        c = self.conn.cursor()
        name = self.tb_name
        cursor = c.execute(f"""
                            CREATE TRIGGER KEEP_ROWS AFTER INSERT ON {name}
                            WHEN (SELECT COUNT(*) FROM {name}) > 10
                              BEGIN
                                 DELETE FROM {name} WHERE ROWID IN 
                                 (SELECT ROWID FROM {name} ORDER BY ROWID LIMIT 1);
                              END;""")

    def create_table(self, list_key):
        str = ''
        for header in self.df_table.columns:
            str += "'" + header + "',"
            self.no_col += 1

        if self.__check_table():
            return

        if list_key:
            str_key = ''
            for ikey in list_key:
                str_key += "'" + ikey + "',"
            str_key = str_key[:-1]
            str_sql = f"""
                CREATE TABLE {self.tb_name}
                (
                  {str}
                  CONSTRAINT CONSTR PRIMARY KEY ({str_key})
                );
            """
        else:
            str = str[:-1]
            str_sql = f"""
                            CREATE TABLE {self.tb_name}
                            (
                              {str}
                            );
                        """

        c = self.conn.cursor()
        # print(str_sql)
        c.execute(str_sql)
        self.__create_trigger()

    def to_db(self):
        str_col = ''
        for i in range(self.no_col):
            str_col += '?,'
        str_col = str_col[:-1]
        c = self.conn.cursor()
        query = f''' insert or replace into {self.tb_name} values ({str_col}) '''
        c.executemany(query, self.df_table.to_records(index=False).tolist())
        self.conn.commit()

def main():
    df = pd.DataFrame({
        'LV0': [2, 1],
        'LV1': [2, 1],
        'LV2': [2, 1],
        'LV3': [2, 1],
        'FAB': ['F6', 'F6'],
        'WEEK': ['W322', 'W323']
    })
    obj = wsqlite(df, r'D:\testdb.db', 'F6_KLA_RECIPE', ['FAB', 'WEEK'])
    obj.to_db()


if __name__=="__main__":
    main()




