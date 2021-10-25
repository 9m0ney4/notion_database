# Last update: 22-10-2021

# Noition Database Read and Save

from notion_client import Client
import pandas as pd 
import json
import os


class Database:
    def __init__(self, token, id):
        self.id = id
        self.token = token
        self.client = Client(auth = self.get_token())
        self.name = self.__name__() # set as the name from the json

    def __name__(self):
        c = self.get_client()
        res = c.databases.retrieve(self.get_id())
        return res['title'][0]['plain_text']

    def get_token(self):
        return self.token

    def get_id(self):
        return self.id
    
    def get_name(self):
        return self.name

    def get_client(self):
        return self.client

    def get_json(self):
        id = self.get_id()
        c = self.get_client()
        next_cursor = None

        current_res = (c.databases.query(**{"database_id": id}))
        all_res = current_res['results']

        while current_res['has_more']:
            next_cursor = current_res['next_cursor']

            current_res = (c.databases.query(**{
                                                    "database_id"  : id,
                                                    "start_cursor" : next_cursor
                                                }))

            all_res += current_res['results']
        return all_res

    def make_df(self):
        # relation & rollup doesnt save in json settings only in data
        res = self.get_json()

        # get a columns list
        c = self.get_client()
        details = c.databases.retrieve(self.get_id())
        df_columns = [col for col in details['properties'].keys()]

        # create the list for the df
        df = []

        for row in res:
            new_row = {col : '' for col in df_columns}
            data_col = {col_name : data for col_name, data in row['properties'].items()}
            for col_name in df_columns:

                # for the missing values
                if col_name not in data_col.keys():
                    continue

                value = self.__get_cell_value__(data_col[col_name])
                if value is None:
                    value = ''
                
                new_row[col_name] = value

            df.append(new_row)

        df = pd.json_normalize(df, max_level=1)

        sort_by_col = 'date'
        if sort_by_col in df.columns:
            # print(f'sorted by {sort_by_col}')
            df.sort_values(by = [sort_by_col])

        return df

    # get the cell json and return the value for the cell
    def __get_cell_value__(self, data):
        # exit condition
        if type(data) in [bool, str, int]:
            return data
        
        elif type(data) == dict:
            # general return
            if 'name' in data:
                return data['name']
            
            # for title and text
            if 'plain_text' in data:
                return data['plain_text']

            # for date
            if 'start' in data:
                return data['start']
            
            col_type = data['type']
            return self.__get_cell_value__(data[col_type])

        elif type(data) == list:
            value = []
            for i in range(len(data)):
                value.append(self.__get_cell_value__(data[i]))
            
            return ','.join(value)
        
        else:
            return None


    def save_json(self, path = None
                      , a = 'data' # data / settings / both 
                      ):
        db_name = self.get_name()

        # to save both:
        if a == 'both':
            self.save_json(path, 'data')
            a = 'settings'

        # to save the DB settings:
        if a == 'settings':
            c = self.get_client()
            data = c.databases.retrieve(self.get_id())

        # to save the json data:
        if a == 'data':
            data = self.get_json()

        if not path:
            path = f'{os.path.dirname(os.path.realpath(__file__))}\{db_name}_{a}.json'

        try:
            with open(path, 'w') as j:
                json.dump(data, j)

        except Exception as ex:
            print(f'the json {a} for {db_name} has not been saved!\n\n{ex}')
            return False

        else:
            return True

    def save_df(self, path = None):
        ...
        

if __name__ == '__main__':
    pass
