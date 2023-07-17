#!/usr/bin/env python
# coding: utf-8

# In[4]:


import numpy as np
import pandas as pd
import warnings


# In[5]:


pd.set_option('display.max_rows',None)
pd.set_option('display.max_columns',None)
warnings.filterwarnings('ignore')


# In[6]:


column_list = ['学历年龄','年齢','勤続年数','所定内実労働時間数','超過実労働時間数','きまって支給する現金給与額(千円)',
               '所定内給与額(千円)','年間賞与その他特別給与額(千円)','労働者数(十人)','year']


# In[7]:


def data_read(sheet_name):
    '''
    function for reading  subdivision industries‘ data from 2009 to 2018 and return a list
    '''

    df_list = []
    for year in range(2009,2019):
    
        
        df = pd.read_excel(f'/Users/minghongsun/Desktop/model/09-18data/中产业data/{year}.xls',sheet_name =sheet_name)
    
        df = df.iloc[12:,8:17]
        df['year'] = int(year)
        df.columns = column_list
        df_list.append(df)
    return df_list
    


# In[8]:


def edu_trans(x):
   '''
       function for converting education background into year of schooling and return a integer data
       
   '''
   
   if x in ['男\u3000 女\u3000 計\n学\u3000 歴   計','\u3000\u3000 男\n学   歴   計','\u3000\u3000 女\n学   歴   計','     男\n学\u3000 歴   計',
           '     女\n学\u3000 歴   計']:
       return 'delete'
   elif x =='中   学   卒':
       return 9
   elif x == '高   校   卒':
       return 12
   elif x == '高専・短大卒':
       return 14
   elif x == '大学・大学院卒':
       return 16


# In[9]:


def sex_trans(x):
    '''
    function for converting sex's janpanese into 0 or 1,0 is female and 1 is male 
    '''
    
    if x in ['男\u3000 女\u3000 計\n学\u3000 歴   計']:
        return 'delete'
    elif x in ['\u3000\u3000 男\n学   歴   計','     男\n学\u3000 歴   計'] :
        return 1
    elif x in ['\u3000\u3000 女\n学   歴   計','     女\n学\u3000 歴   計']:
        return 0


# In[10]:


def del_target_feature(x):
    '''
    function for finding target data out and return a bool data,
    '''
    
    if x in ['中   学   卒','高   校   卒','高専・短大卒','大学・大学院卒'] :
        return False
    else:
        return True


# In[11]:


def data_preprosessing(df_list):
    #concat data in df list into a dataframe
    data = pd.concat(df_list)
    data.reset_index(inplace=True,drop = 'index')
    
    #delete data which age is less than 19 and greater than 60
    del_list = ['\u3000\u3000～１９歳','～１９歳','６０～６４歳','６５～６９歳','７０歳～']
    drop_list = []
    for item in del_list:
        drop_index = data[data['学历年龄'] == item].index
        drop_list.append(drop_index)
    for item in drop_list:
        data.drop(index = item,inplace=True)
    data.reset_index(inplace=True,drop = 'index')
    
    #Call functions ,generate two new featureswhich are gender and educational background and delete useless data   
    data['education'] = data['学历年龄'].map(edu_trans)
    data['sex'] = data['学历年龄'].map(sex_trans)
    data.fillna(method='ffill',inplace=True)
    drop_list = []
    drop_index = data[(data['sex'] == 'delete' )| (data['education']=='delete')].index
    data.drop(index = drop_index,inplace=True)
    data.reset_index(inplace=True,drop = 'index')
    data = data[data['学历年龄'].map(del_target_feature)]
    data.reset_index(drop = 'index',inplace=True)
    
    return data


# In[12]:


def restore_data_in_dic(mul_index,data):
    '''
    Split the mul_index data into e_a(学歴年齢),year,edu,sex and restore them into a dict,
    return is a dict
    '''
    
    #('２０～２４歳', 2009,  9, 1)
    
    elements_dic = {}
    
    for item in mul_index:
        
        e_a,year,edu,sex = item 
        
        if not elements_dic.get(e_a):
            elements_dic[e_a] = {}
        
        if not elements_dic[e_a].get(edu):
            elements_dic[e_a][edu] = {}
            
        if not elements_dic[e_a].get(edu).get(sex):
            elements_dic[e_a][edu][sex] = []
        
        elements_dic[e_a][edu][sex].append(year)
    return elements_dic
    
    


# In[13]:


def find_location_of_filling_data(data,e_a,edu,sex,year):
    '''
    Find location of null data which are groupby e_a,edu,sex,year and return index
    '''
    cond = (data['学历年龄']==e_a) & (data['year']==year) & (data['education']==edu) & (data['sex']==sex)
    fill_index = data[cond].index
    return fill_index


# In[14]:


def cal_filled_data(data,e_a,edu,sex,year_lis):
    '''
    calculate averange  of data whose year don't equcal year in year_lis and return a group of data which data format
    is list
    '''
    
    for year in year_lis:
        
        cond = (data['学历年龄']==e_a) & (data['year'] != year) & (data['sex']==sex) & (data['education']==edu)
        data = data[cond]
    be_filled_data = data.mean()[:-3].values
    return be_filled_data


# In[15]:


def fill_null_data(mul_index,data):
    '''
    Fill null data and return a dataframe
    
    '''
    
    elements_dic = restore_data_in_dic(mul_index,data)
    
    for e_a,value in elements_dic.items():
        
        for edu,item in value.items():
            
        
            for sex,year_lis in item.items():
                
                
                for year in year_lis:

                    fill_index = find_location_of_filling_data(data,e_a,edu,sex,year)  #find location of filling data
                    
                    
                    be_filled_data = cal_filled_data(data,e_a,edu,sex,year_lis)  #calculate  data for filling null data
                    

                    data.iloc[fill_index,1:-3]  = be_filled_data  #fill null data
                    
                    print('*'*100)
                    print('Paragrams for filling null data:')
                    print('-'*50)
                    print('学历年龄 :',e_a)
                    print(f'Sex:{sex}')
                    print(f'Education year:{edu}')
                    print(f'Year of filling null data:{year_lis}')
                    print('-'*50)
                    print(f'Fill value is :{be_filled_data}')
                    print('-'*50)
                    print(f'index location of filling null data:{fill_index}')
            
                  
                    

    return data


# In[16]:


def cal_x_hat(x,min_range,max_range):
    lis = []
    if (x < max_range) and (x >= min_range):
        lis.append(x)
    x_mean = np.mean(lis)
    return x_mean


# In[17]:


def cal_x_hat(series,min_range,max_range):
    lis = []
    for item in series.values:
        if (item< max_range) and (item >= min_range):
            lis.append(item)
    x_mean = np.mean(lis)
    return np.around(x_mean,2)


# In[18]:


def cal_x_hats(data_s):
    '''
    function for calculating a series of x hat,as a input parameter , data_s must be a Series
    return a list containg x hats between 0 and 40
    '''
    x_range = [item for item in range(0,40,2)]
    x_hat_lis = []
    for ind in range(len(x_range)-1):
        x_hat = cal_x_hat(data_s,x_range[ind],x_range[ind+1])
        x_hat_lis.append(x_hat)
    x_hat_series = pd.Series(x_hat_lis)
    del_index = x_hat_series[x_hat_series.isna()].index
    x_hat_series.drop(index = del_index,inplace=True)
    
    return list(x_hat_series.values)

    


# In[ ]:





# In[ ]:





# In[ ]:




