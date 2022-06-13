from pyparsing import col
from simple_salesforce import Salesforce
import datetime
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
load_dotenv()

# connect to salesforce
sf=Salesforce(username=os.getenv('username'),password=os.getenv('password'),security_token=os.getenv('security_token'))
           

#get current date - 30 days
def get_search_date(): #
    today = datetime.date.today() #get current date
    search_date = today -datetime.timedelta(days=30) #get date 30 days ago
    date = str(search_date) + "T00:00:00.000Z" #convert date to string
    return date

#save the data to csv
def save_data(data,filename,mode='w'): #save data to csv
    data.to_csv(filename, index=False,mode=mode) 

#load old data from csv
def load_data():
    data = pd.read_csv('project_data.csv') 
    return data

#get pm names and update dataframe
def update_data_with_pm_name(data): 
    pm_id = data.LastModifiedById.unique() #get unique pm ids
    for each in pm_id: #iterate through pm ids
        query2 = "SELECT Name FROM i360__Staff__c WHERE i360__User__c = '"+each+"'" #query salesforce for pm name
        fetch_results = sf.query_all(query2) #get pm name
        data.replace(each,fetch_results['records'][0]['Name'],inplace=True) #replace pm id with pm name
    return data

#Query salesforce for data and return it as a dataframe
def soql_query(soql):
    fetch_result = sf.query_all(soql) #get data from salesforce
    data = pd.DataFrame(fetch_result['records']) #convert data to dataframe
    data.drop(['attributes'], axis=1, inplace=True) #drop attributes column
    data = update_data_with_pm_name(data) #update data with pm names from salesforce
    return data       

#Compare old data with new data and return a dataframe with differences between old and new data and return a dataframe with new data    
def compare_data(old_data,new_data):
    # columns = ['i360__Project_Number__c','supportworks__Install_Date__c','i360__Customer_Name__c','i360__Market_Segment__c','LastModifiedById','i360__Status__c'] #columns for labels
    new_projects= pd.DataFrame() #create dataframe for new projects
    changed_projects = pd.DataFrame() #create dataframe for changed projects
    for index, row in new_data.iterrows(): #iterate through new data
        datarow = old_data[old_data['i360__Project_Number__c'] == row['i360__Project_Number__c']] #search for project number from new data in old data
        if datarow.shape[0] == 0: #if project number is not found in old data, add it to new projects
            # print(row) #print new project number to console for debugging
            new_projects.loc[len(new_projects.index)] = row #add new project to new projects dataframe    
        else:
            if row['i360__Project_Number__c'] in old_data['i360__Project_Number__c'].values: #if project number is found in old data, check if any fields have changed
                for index, row2 in old_data.iterrows(): #iterate through old data
                    if row2['i360__Project_Number__c'] == row['i360__Project_Number__c']:  #if project number is found in old data, check if any fields have changed
                        if row2['i360__Status__c'] != row['i360__Status__c'] or row2['supportworks__Install_Date__c'] != row['supportworks__Install_Date__c']:  
                            # print(row) #print changed project number to console for debugging
                            # changed_projects.loc[len(changed_projects.index)] = row #add old project to changed projects dataframe ##
                            # print(row2) #print old project number to console for debugging
                            changed_projects.loc[len(changed_projects.index)] = row2 #add changed project to changed projects dataframe
                            

    save_data(new_projects,'new_projects.csv','a') #save new projects to csv
    save_data(changed_projects,'changed_projects.csv','a') #save changed projects to csv

    
#data to query    
query = "SELECT i360__Project_Number__c,supportworks__Install_Date__c,i360__Customer_Name__c,i360__Market_Segment__c,LastModifiedById,i360__Status__c FROM i360__Project__c WHERE supportworks__Install_Date__c >= "+get_search_date()+" AND i360__Status__c != 'Completed' ORDER BY supportworks__Install_Date__c ASC"
data = soql_query(query) #query salesforce for data and return it as a dataframe

old_data = load_data() #load old data from csv

compare_data(old_data,data) #compare old data with new data

save_data(data,'project_data.csv') #save new data to csv REPLACING old data

