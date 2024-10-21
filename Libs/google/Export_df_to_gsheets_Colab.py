import sys
import pandas as pd
import numpy as np

def Delete_Data_on_Sheets(gsheetId, sheetId, gc):
    sheet = gc.open_by_key(gsheetId).worksheet(sheetId.split('!')[0])
    cell_range = sheetId.split('!')[1]
    sheet.batch_clear([cell_range])
    
    print(f'Data in range {cell_range} Deleted')
    
def Export_Data_To_Sheets(gsheetId, sheetId, df, gc):
    worksheet = gc.open_by_key(gsheetId).worksheet(sheetId.split('!')[0])
    cell_range = sheetId.split('!')[1]
    values = df.T.reset_index().T.values.tolist()
    worksheet.update(cell_range, values)

    return print('Data Exported to:', cell_range)

def Import_data_from_Sheets(gsheetId, sheetId, gc):
    worksheet = gc.open_by_key(gsheetId).worksheet(sheetId.split('!')[0])
    cell_range = sheetId.split('!')[1]
    data = worksheet.get(cell_range)
    df = pd.DataFrame(data[1:], columns=data[0])

    return df
    
def Date_formatting(gsheetId, sheetId, values, service):
    sheet = service.open_by_key(gsheetId).worksheet(sheetId)
    sheet.update('A1', values)
    
    return np