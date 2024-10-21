# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 21:21:25 2022

@author: Garena
"""

from Google import Create_Service
import pandas as pd
import numpy as np

def login_token(CLIENT_SECRET_FILE_SOMEONE):
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    print("Chegou no service do google")
    
    service = Create_Service(CLIENT_SECRET_FILE_SOMEONE, API_SERVICE_NAME, API_VERSION, SCOPES)
    
    print("Passou")
    
    return service

def Delete_Data_on_Sheets(gsheetId, sheetId, service):
    request = service.spreadsheets().values().clear(
        spreadsheetId=gsheetId, 
        range= sheetId, 
        ).execute()
    return('Data Deleted')
    
def Export_Data_To_Sheets(gsheetId,sheetId,df,service):
    
    response_date = service.spreadsheets().values().append(
        spreadsheetId=gsheetId,
        valueInputOption='RAW',
        range=sheetId,  
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()
    
    return(print('Data Exported'))

def Import_data_from_Sheets(gsheetId,sheetId,service):
    result = service.spreadsheets().values().get(
        spreadsheetId = gsheetId, 
        range=sheetId).execute()
    
    #Transformando Json em DF pythoniana (lol)
    values = result.get('values', [])
    df = pd.DataFrame(values[1:], columns=values[0])
    
    return df

def Update_data_from_Sheets(gsheetId,sheetId,service):
    result = service.spreadsheets().values().update(
        spreadsheetId=gsheetId, 
        range=sheetId).execute()
    
    return np

def Date_formatting(gsheetId, sheetId,values, service):
    result = service.spreadsheets().values().update(
        spreadsheetId=gsheetId, 
        range=sheetId,
        valueInputOption = "USER_ENTERED",
        # responseDateTimeRenderOption='SERIAL_NUMBER',
        body={"values": values}
    ).execute()

    return np