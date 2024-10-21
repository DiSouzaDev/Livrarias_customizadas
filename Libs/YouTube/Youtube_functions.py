# -*- coding: utf-8 -*-
"""
Created on Sat Mar  4 02:40:33 2023

@author: calild
"""
from pprint import pprint
from google.Google import Create_Service
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

#Infos para abrir o chamado

def login_token_YT(CLIENT_SECRET_FILE_SOMEONE):
    API_SERVICE_NAME = 'youtube'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/youtube']
    
    service = Create_Service(CLIENT_SECRET_FILE_SOMEONE, API_SERVICE_NAME, API_VERSION, SCOPES)
    
    return service

# =============================================================================
# Inserindo os Links para receber os IDs
# =============================================================================
def Link_retorna_channelId(New_Links, service_YT): 
    import requests
    from bs4 import BeautifulSoup
    
    New_Links['Channel ID'] = ''
    
    for i in range(len(New_Links)):
        try: 
            resp = requests.get(New_Links['YT channel'][i])
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            channel_id = soup.select_one('meta[property="og:url"]')['content'].strip('/').split('/')[-1]
            New_Links['Channel ID'][i] = channel_id
            
        except:
            print('Nao Encontrado')
            New_Links['Channel ID'][i] = ''
    
    return New_Links
# =============================================================================
# # API de vídeo para pegar stats dos vídeos
# =============================================================================
def get_video_stats(new_Videos, service_YT): 
    
    #função que a API vai chamar
    part_string = 'contentDetails,statistics,snippet,topicDetails'
    
    column_list = new_Videos['videoId'].tolist()  # Convert the column to a list
    
    # Break the list into sublists of 50 items each
    sublists = [column_list[i:i+50] for i in range(0, len(column_list), 50)]

    base_videos = pd.DataFrame(columns=['videoId','channelId','publishedAt','videoTitle',
                               'videoDescription','videoTag','videoViewCount','videoLikeCount','videoCommentsCount',
                               'duration','channelTitle'])
    #Criando base para receber dados
    for x in range(len(sublists)):
    
        string_list = ','.join(map(str, sublists[x]))
    
        #Abertura do serviço da API
        response = service_YT.videos().list(
        part=part_string,
        id=string_list
        ).execute()
        
        #Alocando as informações do Json em um DF
        for i, video in enumerate(response['items']):
            videoId = sublists[x][i]
            videoTitle = video['snippet']['localized']['title']
            videoViewCount = video['statistics'].get('viewCount', 0)
            videoLikeCount = video['statistics'].get('likeCount', 0)
            videoCommentsCount = video['statistics'].get('commentCount', 0)
            channelTitle = video['snippet']['channelTitle']
            videoDescription = video['snippet']['description']
            channelId = video['snippet']['channelId']
            videoTag = video['snippet'].get('tags', 'Sem Tag')
            duration = video['contentDetails']['duration']
    
            df = pd.DataFrame({'videoId': [videoId],
                  'channelId': [channelId],
                  'publishedAt': [video['snippet']['publishedAt']],
                  'videoTitle': [videoTitle],
                  'videoDescription': [videoDescription],
                  'videoTag': [videoTag],
                  'videoViewCount': [videoViewCount],
                  'videoCommentsCount': [videoCommentsCount],
                  'videoLikeCount': [videoLikeCount],
                  'duration': [duration],
                  'channelTitle': [channelTitle]})
    
            #base_videos = base_videos.append(df, ignore_index=True)
            base_videos = pd.concat([base_videos, df], ignore_index=True)
            
        #talvez isso dê merda na hora de enviar via sheets
        
    base_videos['publishedAt'] = pd.to_datetime(base_videos['publishedAt']).dt.strftime('%Y-%m-%d %H:%M:%S')
    base_videos['videoTag'] = np.where(base_videos['videoTag'] == 'Sem Tag', 'Sem Tag',base_videos['videoTag'].apply(lambda x: ', '.join(map(str, x))))

    base_videos.drop_duplicates(inplace = True)
    
    return base_videos

# =============================================================================
# #API de canal para pegar detalhes do canal
# =============================================================================

def get_channel_stats(channel_List, service_YT): 
    
    column_list = channel_List['Channel ID'].tolist()  # Convert the column to a list
      
    # Break the list into sublists of 50 items each
    sublists = [column_list[i:i+50] for i in range(0, len(column_list), 50)]

    #Criando base para receber dados
    base_channel_stats = pd.DataFrame(columns= ['ChannelID',
                      'SubscriberCount',
                      'ChannelVideoCount',
                      'ChannelViewCount',
                      'ChannelTitle',
                      'ChannelDescription',
                      'ChannelUrl'])


    #Criando base para receber dados
    for x in range(len(sublists)):
        print (x)
        print(len(sublists[x]))
        # Convert each sublist to a string           
        channel_ids = ','.join(map(str, sublists[x]))
        
        #função que a API vai chamar
        part_string = 'contentDetails,statistics,snippet'
        
        #Abertura do serviço da API
        response = service_YT.channels().list(
        part=part_string,
        id=channel_ids
        ).execute()
        
        iterable =  response['pageInfo']['totalResults']
        
        #Alocando as informações do Json em um DF
        for i in range(iterable):
            
            #Subscriber is optional to be blocked, so try/except to don't break code
            try:
                subscriberCount = response['items'][i]['statistics']['subscriberCount']
            except:
                subscriberCount = 'Bloqueado'
    
            channelTitle =        response['items'][i]['snippet']['title']    
            channelVideoCount =   response['items'][i]['statistics']['videoCount']
            channelViewCount =    response['items'][i]['statistics']['viewCount']
            channelUrl =          response['items'][i]['snippet']['customUrl']
            channelDescription =  response['items'][i]['snippet']['description']
            ChannelID =           response['items'][i]['id']
            
            df = pd.DataFrame({'ChannelID':[ChannelID],
                  'SubscriberCount': [subscriberCount],
                  'ChannelVideoCount' :[channelVideoCount],
                  'ChannelViewCount' : [channelViewCount],
                  'ChannelTitle':[channelTitle],
                  'ChannelDescription' : [channelDescription],
                  'ChannelUrl': [channelUrl],
                  })
                  
            #base_channel_stats = base_channel_stats.append(df,ignore_index=True)
            base_channel_stats = pd.concat([base_channel_stats, df],ignore_index=True)

    # Inserting the data column and making it adaptable for editing
    base_channel_stats.insert(0, 'UpdateDate',datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    datas = base_channel_stats[['UpdateDate']].values.tolist()
    column_titles = ["UpdateDate"]
    datas.insert(0, column_titles)
    
    return base_channel_stats, datas

# =============================================================================
# #Chamar vídeos que o canal postou
# =============================================================================

def get_new_videos(channel_List, days_backwards, service_YT): 
    
    column_list = channel_List['Channel ID'].tolist()  # Convert the column to a list

    #Função que API usará
    part_string = 'contentDetails,snippet'
    
    #Ajustando quantos dias voltará ao formato Json
    pulblishedAfter = datetime.now() - timedelta(days_backwards)
    pulblishedAfter = pulblishedAfter.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    #Criando DF
    base_novos_videos = pd.DataFrame(columns=['channelId','videoId','publishedAt'])
    
    #Alocando info ao DF
    for x in column_list:
        response = service_YT.activities().list(
        part=part_string,
        channelId = x,
        publishedAfter = pulblishedAfter,
        maxResults = 50
        ).execute()
            
        for i in range(len(response['items'])):
            try:
                videoId = response['items'][i]["contentDetails"]["upload"]["videoId"]
                publishedAt =      response['items'][i]['snippet']['publishedAt']
                channelId   = response['items'][i]['snippet']['channelId']
                df = pd.DataFrame({'videoId': [videoId],
                      'publishedAt':[publishedAt],
                      'channelId':[channelId]
                      })
                          
                #base_novos_videos = base_novos_videos.append(df,ignore_index=True)
                base_novos_videos = pd.concat([base_novos_videos, df],ignore_index=True)
                
            except:
                print('no new videos')
    
    return base_novos_videos

def get_videos_through_topic(theme, service_YT):
    # theme = 'sigma jogo'  # replace with your desired theme
    #Abertura do serviço da API
    response = service_YT.search().list(
    part='id',
    q=theme,
    type = 'video, channel, playlist',
    maxResults = 50
    ).execute()
    
    # https://developers.google.com/youtube/v3/docs/search/list?hl=pt-br
    # Livraria para encontrar todos os tipos de afunilamento para busca
    
    video_ids = [item['id']['videoId'] for item in response['items']]
    
    #função que a API vai chamar
    part_string = 'contentDetails,statistics,snippet'
    
    #Abertura do serviço da API
    response = service_YT.videos().list(
    part=part_string,
    id=video_ids
    ).execute()
    
    #Criando base para receber dados
    base_videos_buscados = pd.DataFrame(columns=['videoId','videoCommentsCount','videoViewCount','videoLikeCount',
                               'channelTitle','videoDescription','channelId','publishedAt',
                               'videoTag','duration'])
    
    #Alocando as informações do Json em um DF
    for i in range(len(video_ids)):
        videoCommentsCount = response['items'][i]['statistics']['commentCount']
        videoViewCount =   response['items'][i]['statistics']['viewCount']
        try:
            videoLikeCount =        response['items'][i]['statistics']['likeCount']
        except:
            videoLikeCount = 0
        channelTitle =     response['items'][i]['snippet']['channelTitle']
        videoDescription = response['items'][i]['snippet']['description']
        channelId =        response['items'][i]['snippet']['channelId']
        publishedAt =      response['items'][i]['snippet']['publishedAt']
        
        try:
            videoTag =         response['items'][i]['snippet']['tags']
        except:
            videoTag = 'Sem Tag'
            
        duration =         response['items'][i]['contentDetails']['duration']

        df = pd.DataFrame({'videoCommentsCount': [videoCommentsCount],
              'videoViewCount':[videoViewCount],
              'videoLikeCount' :[videoLikeCount],
              'channelTitle' : [channelTitle],
              'videoDescription': [videoDescription],
              'channelId' : [channelId],
              'publishedAt': [publishedAt],
              'videoTag' : [videoTag],
              'duration' : [duration]
              })
              
        #base_videos_buscados = base_videos_buscados.append(df,ignore_index=True)
        base_videos_buscados = pd.concat([base_videos_buscados, df],ignore_index=True)
        
    return base_videos_buscados
   
def tabula_ids(channel_List, all_stats, channelIds, service_YT):
    channelIds['Indice'] = range(len(channelIds))
    channelIds = pd.merge(channel_List, all_stats[['ChannelID', 'SubscriberCount']], how='outer', left_on='Channel ID', right_on='ChannelID')
    channelIds.sort_values('Indice', inplace=True)
    channelIds.fillna('', inplace=True)
    channelIds.drop(['Indice'], axis=1, inplace=True)
    channelIds.drop(['YT channel'], axis=1, inplace=True)
    channelIds.drop(['ChannelID'], axis=1, inplace=True)
    #channelIds.drop_duplicates(subset=['Channel ID'], keep=False, inplace=True)
    
    return channelIds