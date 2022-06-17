from sys import audit #import audit module
from xml.etree.ElementTree import tostring #import tostring
from pyparsing import col
from simple_salesforce import Salesforce #import simple_salesforce
import datetime #import datetime module
import pandas as pd #import pandas
import os #import os module
from dotenv import load_dotenv #import dotenv module
import logging #import logging module
#email imports
import smtplib #import smtplib
from email.mime.text import MIMEText #import MIMEText
from email.mime.multipart import MIMEMultipart #import MIMEMultipart
from email.mime.application import MIMEApplication #import MIMEApplication

from jinja2 import Environment, FileSystemLoader, select_autoescape, PackageLoader

load_dotenv() #load dotenv

env = Environment(loader=PackageLoader('project_audit', 'templates'), #set jinja2 environment'), 
                  autoescape=select_autoescape()) #set autoescape

template = env.get_template('template.html') #get template

# connect to salesforce
sf=Salesforce(username=os.getenv('username'),password=os.getenv('password'),security_token=os.getenv('security_token'))
           
logging.basicConfig(filename ='project_audit.log', level=logging.INFO) #set logging level


#get current date - 30 days
def get_search_date(): #
    today = datetime.date.today() #get current date
    search_date = today -datetime.timedelta(days=30) #get date 30 days ago
    date = str(search_date) + "T00:00:00.000Z" #convert date to string
    return date

#save the data to csv
def save_data(data,filename,mode='w'): #save data to csv
    data.to_csv(filename, index=False,mode=mode)  #save data to csv

#load old data from csv
def load_data(filename):
    data = pd.read_csv(filename)  #load data from csv
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

def email_results():
    
    datetime_now = datetime.datetime.now().strftime("%Y-%m-%d") #get current date
    changed_datetime = datetime.datetime.now() - datetime.timedelta(days=1) #get date 1 days ago
    changed_datetime = changed_datetime.strftime("%Y-%m-%d") #convert date to string
    changed_files = [] #create empty list
    # Create a text/plain message
    msg = MIMEMultipart()
    msg['Subject'] = 'Project Audit: '+datetime_now #set subject
    msg['From'] = os.getenv('email_username') #set from
    msg['To'] = os.getenv('mailto_list') #set to
    # Create the body of the message (a HTML message)
    with open('audit/changed_projects.csv', 'rb') as fp:
        for each in fp:
            changed_files.append(each.decode('utf-8').strip())
    if len(changed_files) > 1:
        html = '<html><body><p>The following projects have been updated since: '+changed_datetime+'</p><ul>' #create html body
        for each in changed_files[1:]: #iterate through changed files
            html += '<li>'+each['i360__Project_Number__c']+'</li>' #add each file to html body
        html += '</ul></body></html>' #close html body
        body = MIMEText(html, 'html') #set body to html
        msg.attach(body) #attach body to message
    else: #if no files have been updated
        body = MIMEText('No projects have been updated since: '+changed_datetime) #set body to text
    msg.attach(body) #attach body to message

    #Attach Files
    for file in os.listdir('./audit'): #iterate through filenames
        with open('./audit/'+file, 'rb') as fp: #open files
            part = MIMEApplication(fp.read(), Name=file) #create attachment
        part['Content-Disposition'] = 'attachment; filename="%s"' % file #set filename
        msg.attach(part) #attach file

    # Send the message via local SMTP server
    s = smtplib.SMTP('smtp.office365.com', 587) #set smtp server
    s.starttls() #starttls
    s.login(os.getenv('email_username'), os.getenv('email_password')) #login to smtp server
    s.sendmail(msg['From'],msg['To'], msg.as_string()) #send email
    s.quit() #quit smtp server
    
    
def cleanup():
    for file in os.listdir('./audit'): #iterate through filenames
        if file.endswith('.csv') != True:
           logging.info('Expected files not found. Exiting.') #log error
        else:
            os.remove('./audit/'+file) #remove files


#Compare old data with new data and return a dataframe with differences between old and new data and return a dataframe with new data    
def compare_data(old_data,new_data):
    columns = ['i360__Project_Number__c','supportworks__Install_Date__c','i360__Customer_Name__c','i360__Market_Segment__c','LastModifiedById','i360__Status__c'] #columns for labels
    new_projects= pd.DataFrame(columns=columns) #create dataframe for new projects
    changed_projects = pd.DataFrame(columns=columns) #create dataframe for changed projects
    for index, row in new_data.iterrows(): #iterate through new data
        datarow = old_data[old_data['i360__Project_Number__c'] == row['i360__Project_Number__c']] #search for project number from new data in old data
        if datarow.shape[0] == 0: #if project number is not found in old data, add it to new projects
            # print(row) #print new project number to console for debugging
            new_projects.loc[len(new_projects.index)] = row #add new project to new projects dataframe    
          
        else:
            if row['i360__Project_Number__c'] in old_data['i360__Project_Number__c'].values: #if project number is found in old data, check if any fields have changed
                for index, row2 in old_data.iterrows(): #iterate through old data
                    if row2['i360__Project_Number__c'] == row['i360__Project_Number__c']:  #if project number is found in old data, check if any fields have changed
                        if ((row2['i360__Status__c'] != row['i360__Status__c']) and (row2['i360__Status__c'] != "Completed")) or (row2['supportworks__Install_Date__c'] != row['supportworks__Install_Date__c']):  
                            # print(row) #print changed project number to console for debugging
                            # changed_projects.loc[len(changed_projects.index)] = row #add old project to changed projects dataframe ##

                            changed_projects.loc[len(changed_projects.index)] = row2 #add changed project to changed projects dataframe
                                             
    os.makedirs('audit/', exist_ok=True) #create directory for audit files 
    save_data(new_projects,'audit/new_projects.csv') #save new projects to csv
    save_data(changed_projects,'audit/changed_projects.csv') #save changed projects to csv
    return changed_projects


def clean_data(data):
    data.drop_duplicates(inplace=True) #drop duplicates
    data.dropna(inplace=True) #drop na
    data.reset_index(drop=True, inplace=True) #reset index
    return data
    

def prepare_data(data):

    data.rename(columns={'i360__Project_Number__c':'Project Number','supportworks__Install_Date__c':'Install Date','i360__Customer_Name__c':'Customer Name','i360__Market_Segment__c':'Market Segment','LastModifiedById': 'Last Modified By','i360__Status__c':'Status'}, inplace=True) #rename columns
    #convert install date to datetime
    data['Install Date'] = pd.to_datetime(data['Install Date']).dt.strftime('%m-%d-%y /n%H:%M')
    # data = list(data)
    return data


#data to query    
query = "SELECT i360__Project_Number__c,supportworks__Install_Date__c,i360__Customer_Name__c,i360__Market_Segment__c,LastModifiedById,i360__Status__c FROM i360__Project__c WHERE supportworks__Install_Date__c >= "+get_search_date()+" AND i360__Status__c != 'Completed' ORDER BY supportworks__Install_Date__c ASC"

def create_dashboard(data):
    html = template.render(data=data)
    with open ('dashboard.html', 'w') as f:
        f.write(html)




cleanup() #cleanup audit files
data = soql_query(query) #query salesforce for data and return it as a dataframe
old_data = load_data('project_data.csv') #load old data from csv
compare_data(old_data,data) #compare old data with new data
create_dashboard(prepare_data(load_data('audit/changed_projects.csv'))) #create dashboard
# save_data(data,'project_data.csv') #save new data to csv REPLACING old data

# email_results() #email results


  
    
