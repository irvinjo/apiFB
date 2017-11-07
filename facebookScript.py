import urllib3
import json
import sqlite3
import requests
import os
from datetime import datetime

# posts pedidos
nPosts = 100
# usuarios pedidos
nUsers = 100000


def get_page_data(page_id, access_token):
    api_endpoint = "https://graph.facebook.com/v2.10/"
    fb_graph_url = api_endpoint + page_id + "?fields=posts.limit(" + str(nPosts) + "){permalink_url,message,likes.limit(" + str(nUsers) + "){name,link,id,picture{url}}},id&access_token=" + access_token
    try:
        http = urllib3.PoolManager()
        api_response = http.request('GET',fb_graph_url)

        try:
            return json.loads(api_response.data.decode('utf-8'))
        except (ValueError, KeyError, TypeError):
            return "JSON error"
    
    except IOError as e:
        if basattr(e, 'code'):
            return e.code
        elif basattr(e, 'reason'):
            return e.reason

page_id = "1674769549453611" # username or id
token = "137795993535371|Y-v_qKANnKCQTlkVUxtGWLvViVc" # access token
page_data = get_page_data(page_id, token)

print( "VERIFICACIÓN INGRESO DE DATOS A LA API" )

# print(page_data)
# print( "Page Name: " + page_data['name'] )



# Cantidad de posts reales
real_nPosts = len(page_data['posts']['data'])
print(real_nPosts)
# create the table if it is not in the db
conn = sqlite3.connect('facebookData.db')
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS Posts (Post_id real, Mesg_post text, Post_url real, Num_likes real)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Users (Usr_id real, Usr_name text, Usr_url text, Usr_picture_url text)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Likes (Post_id real, Usr_id real)''')

for i in range(real_nPosts):
    
    # Cantidad de usuarios reales
    real_nUsers = len(page_data['posts']['data'][i]['likes']['data'])

    try:
        Mesg_post = page_data['posts']['data'][i]['message']
    except KeyError:
        Mesg_post = "-"

    Post_id = page_data['posts']['data'][i]['id']
    Post_url = page_data['posts']['data'][i]['permalink_url']
    Num_likes = real_nUsers

    for j in range(real_nUsers):
        Usr_id = page_data['posts']['data'][i]['likes']['data'][j]['id']
        Usr_name = page_data['posts']['data'][i]['likes']['data'][j]['name']
        Usr_url = page_data['posts']['data'][i]['likes']['data'][j]['link']
        try:
            Usr_picture_url = page_data['posts']['data'][i]['likes']['data'][j]['picture']['data']['url']
        except KeyError:
            Usr_picture_url = "-"

        print("VERIFICACIÓN INGRESO DE DATOS BD")
        print("Usr ID: ", Usr_id, "Usr Name: ", Usr_name, "Usr URL: ", Usr_url, "Usr_Picture: ", Usr_picture_url)
        for ins1 in [(Usr_id, Usr_name, Usr_url, Usr_picture_url)]:
            print('------------------------------------------------------')
            print(ins1)
            cur.execute('INSERT INTO Users (Usr_id, Usr_name, Usr_url, Usr_picture_url) VALUES (?,?,?,?)', ins1)

        print("Post ID: ", Post_id, "User ID: ", Usr_id)
        for ins2 in [(Post_id, Usr_id)]:
            print('------------------------------------------------------')
            print(ins2)
            cur.execute('INSERT INTO Likes (Post_id, Usr_id) VALUES (?,?)', ins2)

    ins3 = [(Post_id, Mesg_post, Post_url, Num_likes)]
    print('--------------------------------------------------------------')
    print(ins3)
    cur.execute('INSERT INTO Posts (Post_id, Mesg_post, Post_url, Num_likes) VALUES (?,?,?,?)', ins3)

conn.commit()
cur.close()
conn.close()