from flask import Flask, render_template, request, jsonify, make_response,Markup,session,redirect, url_for, Response,g
from flask_restful import Resource, Api
from flask import send_file
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime
import pymysql
import time
import json
import math
import sqlite3 as sql
import os
from passlib.hash import sha256_crypt
from datetime import timedelta
from io import BytesIO


try:
    db_connection_str = 'mysql+pymysql://root:Airtel@123@localhost'
    db_connection = create_engine(db_connection_str)
    print("connection established")
except:
    print("Error")


app = Flask(__name__)
app.config["SECRET_KEY"] = "OCML3BRawWEUeaxcuKHLpw"
app.config['PERMANENT_SESSION_LIFETIME'] =  timedelta(minutes=120)
api = Api(app)

def queryBuilder(database,tableName,word,columnName):
    usersession = session['user']
    session.pop('user', None)
    paramDict = dict()
    # if(word):
    if word == "noword" and columnName == "nocolumn":
        string = f'select * from {database}.{tableName}'
        paramDict =  dict()
    else:
        if len(word)!=0:
            session[columnName] = word
        
        else:
            if(session.get(columnName)):
                session.pop(columnName, None)

        
        string = f'select * from {database}.{tableName} Where'
        
        count = 0
        listOfKeys = session.keys()
        if(len(listOfKeys)>0):
            for key,value in session.items():
                if(value):
                    string = string +' '+ "`"+key.strip()+"`" + " LIKE %(word"+str(count)+")s "
                    paramDict["word"+str(count)] = "%"+value+"%"
                    if count != (len(listOfKeys) - 1):
                        string = string + "AND "
                    count += 1    

                    
                else: 
                    string = ""
            
                    
        else:
            string = ""
    # else:
    #     string = f'select * from {database}.{tableName}'
    #     paramDict =  dict()
    session['user'] = usersession 
    return string, paramDict

def lazy(query,shape,param="none"):
    print('QUERY--->',query,param)
    if param!= "none":
        df1 = pd.read_sql(query, con=db_connection,params=param)
    else:
        df1 = pd.read_sql(query, con=db_connection )
    
    if(len(df1)):
        if 'COLUMNNAMEFORSTRING' in df1.columns:
            columnForLink = df1['COLUMNNAMEFORSTRING'].iloc[0]
            df1.drop(['COLUMNNAMEFORSTRING'], axis=1,inplace=True)
            df1[columnForLink] = df1[columnForLink].apply(lambda x: "{}{}{}".format('<p onClick="testForLinks(this)" style="color:rgb(228, 119, 119);cursor:pointer;display:block;">',x,'</p>') if x else x)
        
        value = df1.to_html( header=False ,index=False,render_links=True,escape=False,)     #time Eater
        value = value.replace('<table border="1" class="dataframe">','')
        value = value.replace('</table>','')
        value = value.replace('</tbody>','')
        value = value.replace('<tbody>','')
        
        for i in range(df1.shape[0]):
            value = value.replace('<tr>','<tr c><td class="index">'+str(i+shape)+'</td>',1)
        
    else:
        value = "<p class='noEntry'><p>"
    return value

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
       
        olmsid = request.form['username'].lower()
        connection = db_connection.connect()
        df1 = connection.execute("SELECT * FROM users.ingress WHERE olmid= %s", (olmsid)).fetchone()
        if(df1):
            if sha256_crypt.verify(request.form['password'], df1[5]):
                connection.execute("UPDATE users.ingress SET visit = %s WHERE olmid= %s", (str(int(df1[6])+ 1),olmsid))
                session['user'] = olmsid
                session.permanent = True
                return redirect(url_for('home'))
            else:
                return render_template('login.html',message="Incorrect Password")
        else:
           return render_template('login.html',message="Incorrect Username")
           
    return render_template('login.html')

@app.route('/home')
def home():
    if g.user:
        return render_template('root.html')

    return redirect(url_for('index'))



@app.route('/database')
def database():
    usersession = session['user']
    session.clear()
    if request.args:
        databaseName = request.args.get("c")
    tableNames =  pd.read_sql('show tables in '+databaseName, con=db_connection )
    tableNamesList = tableNames.to_json()
    res = make_response(tableNamesList, 200)
    session['user'] = usersession
    return res
    
@app.route('/tableHeaders')
def tableHeaders():
    usersession = session['user']
    session.clear()
    if request.args:
        mainData = request.args.get("c")
    data = mainData.split(",")
    database = data[0] 
    table = data[1]
    headers =  pd.read_sql('select * from '+database+'.'+table+' limit 0,1;', con=db_connection )
    headers1 = list(headers.columns)
    if('COLUMNNAMEFORSTRING' in headers1):
        headers1.remove('COLUMNNAMEFORSTRING')
    res = make_response(json.dumps(headers1), 200)
    session['user'] = usersession
    return res

@app.route("/load")
def load():
    if request.args:
        mainData = request.args.get("c")
    data = mainData.split(",")
    print(data)
    database = data[0] 
    table = data[1]
    column = data[2]
    word = data[3]
    counter = int(data[4])
    
    query,params = queryBuilder(database,table,word,column)
    if query:
        print(f"Returning posts {counter} to {counter + 35}")
        res = make_response(jsonify(lazy(query+' limit '+str(counter)+',35;',counter,params)), 200)
    else:
        res = make_response(jsonify(lazy(f"select * from {database}.{table} "+' limit '+str(counter)+',35;',counter)), 200)
    
    return res
    

@app.route('/downloadFile')
def downloadFile():
    if request.args:
        mainData = request.args.get("c")
    data = mainData.split(",")
    database = data[0] 
    table = data[1]
    column = data[2]
    word = data[3]
    query,params = queryBuilder(database,table,word,column)
    
    
    startTime = time.time()
    if(not query):
        query = f"select * from {database}.{table};"
    if params!= "none":
        df1 = pd.read_sql(query, con=db_connection,params=params)
    else:
        df1 = pd.read_sql(query, con=db_connection)
        
    if 'COLUMNNAMEFORSTRING' in df1.columns:
        df1.drop(['COLUMNNAMEFORSTRING'], axis=1,inplace=True)
    buffer = BytesIO()
    buffer.write(df1.to_csv().encode('utf-8'))
    buffer.seek(0)

    return send_file(buffer,mimetype='text/csv',attachment_filename='report.csv',as_attachment=True)
    
@app.route('/tableInfo')
def tableInfo():
    if request.args:
        mainData = request.args.get("c")
    
    data = mainData.split(",")
    database = data[0] 
    table = data[1]
    rows = pd.read_sql(f'select count(*) from {database}.{table}', con=db_connection)['count(*)'].iloc[0]
    size = pd.read_sql(f'SELECT table_name AS `Table`, round(((data_length + index_length) / 1024 / 1024)*4, 2) `Size in MB` FROM information_schema.TABLES WHERE table_schema = "{database}"    AND table_name = "{table}";', con=db_connection)['Size in MB'].iloc[0]
    return str(f"{rows}/{size}")
    
@app.before_request
def before_request():
    g.user = None
    if 'user' in session:
        g.user = session['user']

@app.route('/register',methods=['GET','POST'])
def register():
    if request.method == 'POST':
        olmsid = request.form['username']
        name = request.form['name']
        department = request.form['department']
        manager = request.form['manager']
        password = request.form['password']
        connection = db_connection.connect()
        df1 = connection.execute("SELECT * FROM users.ingress WHERE olmid= %s", (olmsid)).fetchone()
        hashedPassword = sha256_crypt.encrypt(password)
    
        if(not df1):
            connection.execute("INSERT INTO users.ingress (olmid,name,department,manager,password,permission,visit) VALUES(%s,%s,%s,%s,%s,%s,%s);",(olmsid,name,department,manager,hashedPassword,"default",0))
            return render_template('register.html',message = "User Created")
        else:
            return render_template('register.html',message = "User Already Exists")

    return render_template('register.html')

    

@app.route('/dropsession')
def dropsession():
    session.pop('user', None)
    return redirect(url_for('index'))

class fetch(Resource):
    def get(self, database, table):
        df = pd.read_sql(f"select * from {database}.{table}", con=db_connection )
        return df.to_json(orient='split')

api.add_resource(fetch, '/fetch/<database>/<table>')

if __name__ == '__main__':
    app.run(use_reloader= True,debug=True,host='0.0.0.0', port=8011,threaded=True)