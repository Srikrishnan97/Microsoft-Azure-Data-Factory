import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import math
import numpy as np
import datetime
from forex_python.converter import CurrencyRates

def candidate_func():
    # Candidate tab transformations
    global c1,can_res,candidate,interview
    interview = pd.read_csv(R"D:\hari\project\recruitment\local files\interview.csv")
    candidate = pd.read_csv(R"D:\hari\project\recruitment\local files\candidate.csv")
    c1 = candidate[~candidate['Gender'].isnull()]
    c1 = c1[~c1['Status'].isin(['rejected','selected'])]
    c2 = c1[['Candidate_Id']]
    i1  = c2.merge(interview,on = 'Candidate_Id',how= 'inner')
    i2 = i1.groupby(['Candidate_Id'])['Interview_Id'].max()
    can_res = i1[i1['Interview_Id'].isin(i2.values)][["Candidate_Id","Round","Result","Status"]]
    
    sheet = client.open("recruitment sample data").worksheet('candidate')
    can_upd = dict(list(zip(can_res.Candidate_Id.values,can_res.set_index('Candidate_Id').values.tolist())))
    li = []
    for i in can_upd:
        for num,j in enumerate([11,12,13]):
            li.append(gspread.models.Cell(int(i+1),j,str(can_upd[i][num])))
    #print(li)
    log = sheet.update_cells(li)
    log['time'] = str(datetime.datetime.now())
    f = open(r'D:\hari\project\recruitment\logs\logs.csv','a')
    # f.write(','.join(log.keys()))
    f.write('\n')
    f.write(','.join(map(str,log.values())))

    
def referral_func():    
    # referral tab transformations
    r1 = pd.read_csv(R"D:\hari\project\recruitment\local files\referral.csv")
    r1  = r1[~r1['Candidate_Id'].isnull()]
    r1.Candidate_Id = r1.Candidate_Id.astype('int')
    r1.Emp_Id = r1.Emp_Id.astype('int')
    r1 = r1[['Referral_Id','Candidate_Id','Emp_Id']]
    r2 = can_res[can_res['Candidate_Id'].isin(c1[c1['Source'] == 'referral']['Candidate_Id'].values)]
    r3 = r1.merge(r2,on = 'Candidate_Id')[['Referral_Id','Status']]
    ref_upt = dict(zip(r3.Referral_Id.values,r3.Status.values))
    
    sheet = client.open("recruitment sample data").worksheet('referral')
    li = []
    for i in ref_upt:
        li.append(gspread.models.Cell(i+1,5,str(ref_upt[i])))
    log = sheet.update_cells(li)
    log['time'] = str(datetime.datetime.now())
    f = open(r'D:\hari\project\recruitment\logs\logs.csv','a')
    f.write('\n')
    f.write(','.join(map(str,log.values())))

def recruitment_func():
    # recruitment tab transformations
    global employee,recruitment_1,r1
    recruitment = pd.read_csv(R"D:\hari\project\recruitment\local files\recruitment.csv")
    recruitment = recruitment[~recruitment['Role'].isnull()]
    print(recruitment.Total_Requisition.sum())
    print(recruitment.shape)
    employee = pd.read_csv(R"D:\hari\project\recruitment\local files\employee.csv")
    employee = employee[~employee['Candidate_Id'].isnull()]
    e1 = employee.groupby(['Request_Id'],as_index=False)['Emp_Id'].count()
    e1.columns = ['Request_Id','Joined_Cnt']
    recruitment_1 = recruitment.copy()
    recruitment = recruitment[~recruitment['Status'].isin(['on-hold','cancelled','closed'])]
    r1 = recruitment.merge(e1,on = 'Request_Id')
    r1['Balance_Position'] = r1['Total_Requisition'] - r1['Joined_Cnt']

    def status(x):
        if x['Cancelled_Date'] is not np.nan:
            return 'cancelled'
        elif x['Resumed_Date'] is np.nan and x['On_Hold_Date'] is not np.nan:
            return "on-hold"
        elif x['Balance_Position'] > 0:
            return "open"
        else:
            return "closed"

    r1['Status_New'] = r1.apply(status,axis=1)
    r1.On_Hold_Date = pd.to_datetime(r1.On_Hold_Date)
    r1.Resumed_Date = pd.to_datetime(r1.Resumed_Date)
    r1.Opened_On = pd.to_datetime(r1.Opened_On)
    r1['onhold_age'] = (r1['Resumed_Date'] - r1['On_Hold_Date']).dt.days
    r1['today'] = datetime.datetime.now().date()
    r1['today'] = pd.to_datetime(r1['today'])
    r1['normal_ageing'] = (r1['today'] -  r1['Opened_On']).dt.days
    r1.onhold_age = r1.onhold_age.fillna(0)
    r1['ageing'] = r1['normal_ageing'] - r1['onhold_age']
    r1['ageing'] = r1['ageing'].astype('int')
    r1['rec_conver_rt'] = round((r1['Joined_Cnt']/r1['Total_Requisition']) * 100,2)
    recruitment_1 = recruitment_1.merge(r1[['Request_Id','Status_New']],how='left')
    print(r1.Total_Requisition.sum())
    r2 = r1[['Request_Id','Joined_Cnt','Balance_Position','ageing','Status_New','rec_conver_rt']]

    rec_upt = dict(zip(r2.Request_Id.values,r2[['Joined_Cnt','Balance_Position','ageing','Status_New','rec_conver_rt']].values.tolist()))
    sheet = client.open("recruitment sample data").worksheet('recruitment')
    li = []
    for i in rec_upt:
        for num,j in enumerate([16,17,18,19,20]):
                li.append(gspread.models.Cell(i+1,j,str(rec_upt[i][num])))
    log = sheet.update_cells(li)
    log['time'] = str(datetime.datetime.now())
    f = open(r'D:\hari\project\recruitment\logs\logs.csv','a')
    f.write('\n')
    f.write(','.join(map(str,log.values())))

def employee_func():   
    # Employee tab transformations
    e1 = employee.merge(recruitment_1[['Request_Id','Opened_On','On_Hold_Date','Resumed_Date','Cancelled_Date','Status_New']])
    e2 = e1[~e1['Status_New'].isin(['cancelled'])]
    e2['Joined_Date'] = pd.to_datetime(e2['Joined_Date'])
    e2.On_Hold_Date = pd.to_datetime(e2.On_Hold_Date)
    e2.Resumed_Date = pd.to_datetime(e2.Resumed_Date)
    e2.Opened_On = pd.to_datetime(e2.Opened_On)
    #print(e2.dtypes)
    def hire_time(x):
        if (x['On_Hold_Date'] is not pd.NaT) :
            if x['Joined_Date'] <  x['On_Hold_Date']:
                return (x['Joined_Date'] - x['Opened_On']).days
            if  (x['Joined_Date'] >  x['On_Hold_Date']) and (x['Resumed_Date'] is not pd.NaT ):
                return (x['Joined_Date'] - x['Resumed_Date']).days
        else:
            return (x['Joined_Date'] - x['Opened_On']).days

    e2['Hike_Pert'] = round((e2['Salary']/e2['Last_Salary'] - 1) *100)
    e2['Hire_Time'] = e2.apply(hire_time,axis=1)
    e2['Salary_Usd'] =  round((e2['Salary']/usd_inr),2)
    e2 = employee.merge(e2[['Emp_Id','Hire_Time','Salary_Usd']],how='left')
    e2  = e2.sort_values('Emp_Id')
    e2 = e2.merge(candidate[['Candidate_Id','Source']]) 
    #print(e2.columns)
    #print(e2.head(1))
    sheet = client.open("recruitment sample data").worksheet('employee')
    sheet.clear()
    sheet.append_row(list(e2.columns))
    d1 = {}
    li= []
    for i,j in enumerate(e2.values.tolist()):
        d1[i+1] = j
        for num,j1 in enumerate([1,2,3,4,5,6,7,8,9,10,11]):
            val = d1[i+1][num]
            if type(val) == float:
                if math.isnan(val):
                    val = ''
            li.append(gspread.models.Cell(i+1+1,j1,str(val)))
    #print(li)        
    log = sheet.update_cells(li)
    log['time'] = str(datetime.datetime.now())
    f = open(r'D:\hari\project\recruitment\logs\logs.csv','a')
    f.write('\n')
    f.write(','.join(map(str,log.values())))
    

def funnel_func():
    d = dict(zip(interview.Round.value_counts().index.tolist(),interview.Round.value_counts().tolist()))
    d['Offered'] = candidate.Offered.value_counts()['yes']
    d['Joined'] = employee.shape[0]
    d = list(d.values())
    li= []
    temp = [1,2,3,4,5,6,7]
    for i,j in enumerate(d):
            li.append(gspread.models.Cell(2,temp[i],str(j)))
    #print(li)
    sheet = client.open("recruitment sample data").worksheet('funnel')
    log = sheet.update_cells(li)
    log['time'] = str(datetime.datetime.now())
    f = open(r'D:\hari\project\recruitment\logs\logs.csv','a')
    f.write('\n')
    f.write(','.join(map(str,log.values())))
    f.close()
    
def main():
    global client,usd_inr
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(r'D:\hari\project\recruitment\recruitment-sheets-fedf7146289b.json', scope)
    client = gspread.authorize(creds)
    sheets_name = ['recruitment','candidate','referral','interview','employee']
    dic1 = {'recruitment':['Request_Id','Role','Experience','Client','Total_Requisition','Project','Opened_On','Recruiter','Priority','Location','On_Hold_Date','Resumed_Date','Cancelled_Date','Skills','Role_Category','Status','Remarks'],
           'candidate':["Candidate_Id","Name","Gender","Email_Id","Phone_Number","Source","Career_Started_At","Break_Taken","Relevant_Experience",'Round','Result','Status','Offered'],
           'referral':["Referral_Id","Candidate_Id","Emp_Id","Referral_Date",'Status'],
           'interview':["Interview_Id","Candidate_Id","Interviewer_Id","Round","Result","Interview_Date","Status","Remarks"],
           'employee':["Emp_Id","Name","Candidate_Id","Request_Id","Joined_Date","Salary","Last_Salary","Hike_Pert"]}
    c = CurrencyRates()
    usd_inr = round(c.get_rates('USD') ['INR'],2)
    for i in sheets_name:
        # Find a workbook by name and open the sheet
        sheet = client.open("recruitment sample data").worksheet(i)
        # Extract and print all of the values
        list_of_hashes = sheet.get_all_records()
        pd.DataFrame(list_of_hashes)[dic1[i]].to_csv(r"D:\hari\project\recruitment\local files\{}.csv".format(i),index=False)
    candidate_func()
    referral_func()
    recruitment_func()
    employee_func()
    funnel_func()

main()
