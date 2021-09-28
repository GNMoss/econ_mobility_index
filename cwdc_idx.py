# -*- coding: utf-8 -*-
"""
Created on Wed Jul  7 12:05:53 2021

@author: Gabriel Moss
"""
import pandas as pd
import requests
import math
import ast
import numpy as np
import os
from bs4 import BeautifulSoup as bs

def in_demand_occupations():
    '''
    gets in demand occupations from cdhe projections
    
    Returns
    -------
    list containing in demand occupations

    '''
    #read in file
    top = pd.read_csv(working_dir+'All Top Jobs.csv')
    
    #clean median salary and projected openings
    top['Median Annual Salary ($)'] = [int(i.replace(',','')) for i in top['Median Annual Salary ($)']]
    top['Projected Annual Openings'] = [int(i.replace(',','')) for i in top['Projected Annual Openings']]

    #get job zone 1-3
    url = 'https://www.onetonline.org/find/zone?z={}&g=Go'
    
    socs = {}
    
    for i in range(1,3):
        r = requests.get(url.format(i))
        soup = bs(r.content)
        tab = soup.find('table')
        rows = tab.findAll('tr')[1:]
        for r in rows:
            socs[r.find('td').text.split('.')[0]] = 'jz{}'.format(i)

    #read in emsi to onet soc map
    emsiSocs = pd.read_csv(working_dir+'map_stdonet_emsisoc2019.csv')
    emsiSocs.drop_duplicates(inplace=True)
    
    #merge top occupations with the emsi / onet crosswalk, use only 6 digit onet code
    top = top.merge(emsiSocs,left_on='SOC Code',right_on='emsi_soc_5',how='left')
    top['SOC'] = [i.split('.')[0] if str(i) != 'nan' else np.nan for i in top['std_onet']]
    top = top[['SOC Code','SOC','Median Hourly Salary ($)','Median Annual Salary ($)','2019-2029 Growth (%)',
               'Projected Annual Openings']].drop_duplicates()
    
    #iterate through the table and replace blank SOC with corresponding SOC codes
    for index, row in top.iterrows():
        if str(row['SOC']) == 'nan':
            top.loc[index,'SOC'] = row['SOC Code']
    
    #identify job zone 3 occupations
    top['jz'] = [i in socs.keys() for i in top['SOC']]
    top = top[top['jz']].drop(['SOC Code','jz'],axis=1)
    
    #return a list of in demand job zone 3 occupations
    return(list(top['SOC']))

def brookings_occupations():
    '''
    use data from brookings to produce a set of attainable jobs for the front line workforce

    Returns
    -------
    list of soc codes derived from brookings model

    '''
    #read in first step transition data
    bocc1 = pd.read_csv(working_dir+'WoF_CREC_data/full_transition_file_socxx.csv')
    
    #keep only transitions to new occupations
    bocc1 = bocc1[bocc1['occ_a']!=bocc1['occ_b']]
    
    #keep only transitions earning at least the same as the starting occupation
    bocc1 = bocc1[bocc1['h_median_b']>=bocc1['h_median_a']]
    
    bocc1 = bocc1[['occ_a','occ_b']].rename(columns={'occ_b':'occ_c'})
    
    #read in and subset second stage transition data
    bocc2 = pd.read_csv(working_dir+'WoF_CREC_data/full_transition_file_socxx_2_step.csv')
    bocc2 = bocc2[bocc2['occ_a']!=bocc2['occ_c']]
    bocc2 = bocc2[bocc2['h_median_c']>=bocc2['h_median_a']]
    
    bocc2 = bocc2[['occ_a','occ_c']]
    
    #append first and second stage transitions
    bocc = bocc1.append(bocc2)
    
    #read in brookings to SOC crosswalk
    xwalk = pd.read_csv(working_dir+'WoF_CREC_data/full_crosswalk_soc10_socxx.csv')
    
    #crosswalk brookings to SOC code
    bocc = bocc.merge(xwalk,left_on='occ_c',right_on='socxx_code')
    
    #return a list of the unique first and second stage transition occupatinos that did not lose money when transitioning
    return(list(bocc['soc_code'].unique()))

def in_demand_cips():
    '''
    produces a list of CIP codes based on a list of in_demand occupations

    Returns
    -------
    list of CIP codes related to in_demand occupations

    '''
    #generate list of SOC codes
    socs = in_demand_occupations()
    
    #read in cip to soc crosswalk
    soc_x_cip = pd.read_excel(working_dir+'CIP2020_SOC2018_Crosswalk.xlsx',sheet_name='SOC-CIP')
    
    #generate list of CIP codes and remove decimal place
    cips = list((soc_x_cip['CIP2020Code'][soc_x_cip['SOC2018Code'].isin(socs)].drop_duplicates() * 10000).astype(int))
    
    return(cips)

def brookings_opporunity_cips():
    '''
    produces a list of CIP codes based on a list of brookings occupations

    Returns
    -------
    list of CIP codes related to brookings occupations

    '''
    #generate list of SOC codes
    socs = brookings_occupations()
    
    #read in cip to soc crosswalk
    soc_x_cip = pd.read_excel(working_dir+'CIP2020_SOC2018_Crosswalk.xlsx',sheet_name='SOC-CIP')
    
    #generate list of CIP codes and remove decimal place
    cips = list((soc_x_cip['CIP2020Code'][soc_x_cip['SOC2018Code'].isin(socs)].drop_duplicates() * 10000).astype(int))
    
    return(cips)

def related_industries():
    '''
    generates list of 6 digit industry codes where front line workers
    can earn equal to or greater than they currently earn in retail and accomodation
    industries

    Returns
    -------
    list of related industries

    '''
    #read in cwdc front line occupations
    cwdc_socs = pd.read_csv(working_dir+'cwdc_socs.txt',sep='|',header=None)
    cwdc_socs = [i.split()[0] for i in cwdc_socs[0]]

    #read in oes research staffing patterns
    oes = pd.read_excel(working_dir+'oes_research_2020_allsectors.xlsx')
    
    #clean oes data
    oes = oes[oes['AREA'] == 8]
    oes.replace('*',np.nan,inplace=True)
    oes.replace('**',np.nan,inplace=True)    
    oes.replace('~',np.nan,inplace=True)
    oes.replace('#',np.nan,inplace=True)
    
    #subset to just front line occupations
    oes = oes[oes['OCC_CODE'].isin(cwdc_socs)]
    
    #drop sector level industries
    oes = oes[oes['I_GROUP']!='sector']
    
    #flag retail / accomodation industries
    oes['ret_accom'] = [str(i)[:2] in ['44','45','71','72'] for i in oes['NAICS']]
    
    #calculate median earnings for retail and accomodation industry workers
    ret_accom_earn = oes[oes['ret_accom']]['A_MEDIAN'].median()
    
    #subset to non retail / accomodation industries where workers earn more than retail / accom median wage
    oes = oes[(~oes['ret_accom']) & (oes['A_MEDIAN'] > ret_accom_earn)]

    #return list of unique NAICS codes meeting above criteria
    rel_ind = list(oes['NAICS'].unique())
    
    return(rel_ind)
    
def ipeds(year):
    '''
    gather ipeds data through the urban API

    Parameters
    ----------
    year : int
        most recent year of available data

    Returns
    -------
    data frame containing postsecondary and primary data on completers, programs, schools, and absenteeism

    '''
    
    #directory end point
    call = "https://educationdata.urban.org/api/v1/college-university/ipeds/directory/{}/?fips=8".format(year)
    
    m = []
    
    a = True
    
    #loop through directory responses, recording the unit id and fips code for all postsecondary ed insts in colorado
    while a:
        response = requests.get(call)
        directory = response.json()
    
        for col in directory['results']:
            m.append({'unitid' : col['unitid'], 'fips' : col['county_fips']})
        call = directory['next']
        
        a = call is not None
    
    #format data into a metadata dataframe
    meta = pd.DataFrame(m)
    
    #identify colleges in colorado
    unitid = [str(i) for i in meta['unitid']]
    
    #end point for 6 digit CIP code completer data
    call = "https://educationdata.urban.org/api/v1/college-university/ipeds/completions-cip-6/{}/?sex=99&race=99&majornum=1&unitid={}".format(year,','.join(unitid))
    
    h = []
    
    a = True
    #loop through responses, record unitid, cip code, and awards
    while a:
        response = requests.get(call)
        data = response.json()
    
        for col in data['results']:
            h.append({'unitid' : col['unitid'],'cipcode_6digit' : col['cipcode_6digit'], 'awards' : col['awards']})
        call = data['next']
        
        a = call is not None
    
    #format into data frame
    df = pd.DataFrame(h)
    
    #merge data with metadata on unit id and drop total completers
    cwdc_ipeds = meta.merge(df,on='unitid')
    cwdc_ipeds = cwdc_ipeds[cwdc_ipeds['cipcode_6digit']!=99]
    
    #create list of in_demand cip codes
    op_cip = in_demand_cips()
    
    #identify in_demand cips
    cwdc_ipeds['op_cip'] = [i in op_cip for i in cwdc_ipeds['cipcode_6digit']]
    
    #count programs and completers by fips code
    op_prog = pd.DataFrame(cwdc_ipeds[cwdc_ipeds['op_cip']].groupby('fips')['cipcode_6digit'].count()).rename(columns={'cipcode_6digit':'in_demand_programs'})
    op_comp = pd.DataFrame(cwdc_ipeds[cwdc_ipeds['op_cip']].groupby('fips')['awards'].sum()).rename(columns={'awards':'in_demand_awards'})

    #join completers and program counts
    in_demand = op_prog.join(op_comp,how='outer')
    
    #repeat above steps with brookings occupations
    b_op_cip = brookings_opporunity_cips()
    
    cwdc_ipeds['b_op_cip'] = [i in b_op_cip for i in cwdc_ipeds['cipcode_6digit']]
    
    b_op_prog = pd.DataFrame(cwdc_ipeds[cwdc_ipeds['b_op_cip']].groupby('fips')['cipcode_6digit'].count()).rename(columns={'cipcode_6digit':'opporunity_programs'})
    b_op_comp = pd.DataFrame(cwdc_ipeds[cwdc_ipeds['b_op_cip']].groupby('fips')['awards'].sum()).rename(columns={'awards':'opportunity_awards'})

    b_opportunity = b_op_prog.join(b_op_comp,how='outer')
    
    #count number of institutions per county
    colleges = pd.DataFrame(cwdc_ipeds[['fips','unitid']].drop_duplicates().groupby('fips')['unitid'].count()).rename(columns={'unitid':'postsec_inst_count'})
    
    #chronic absenteeism
    
    #primary school endpoint
    call = "https://educationdata.urban.org/api/v1/schools/ccd/directory/{}/?fips=8".format(year)
    
    m = []
    
    a = True
    
    #loop through dictionary, record ncessch, fips, and enrollment
    while a:
        response = requests.get(call)
        directory = response.json()
    
        for col in directory['results']:
            m.append({'ncessch' : col['ncessch'], 'fips' : col['county_code'], 'enrollment' : col['enrollment']})
        call = directory['next']
        
        a = call is not None
    
    #format into metadata dataframe
    meta = pd.DataFrame(m)
    
    #absenteeism endpoint    
    call = "https://educationdata.urban.org/api/v1/schools/crdc/chronic-absenteeism/2015/race/sex/?sex=99&race=99&fips=8"
    
    h = []
    
    a = True
    
    #loop through results, record ncessch, students chronically absent
    while a:
        response = requests.get(call)
        data = response.json()
        
        for col in data['results']:
            h.append({'ncessch' : col['ncessch'],'students_chronically_absent' : col['students_chronically_absent']})
        call = directory['next']
        
        a = call is not None

    #format into data frame
    df = pd.DataFrame(h)
    
    #merge with metadata
    absenteeism = meta.merge(df,on='ncessch').groupby('fips')['students_chronically_absent','enrollment'].sum().reset_index()
    absenteeism['fips'] = absenteeism['fips'].astype(int)
    absenteeism.set_index('fips',inplace=True)
    
    #calculate the rate of absentee students per student enrolled
    absenteeism['absentee_rt'] = absenteeism['students_chronically_absent'] / absenteeism['enrollment']
    
    #join college count, absenteeism, in_demand cip code and brookings cip code data together
    out = colleges.join([absenteeism,in_demand,b_opportunity],how='outer').fillna(0)
    
    return(out)

def acs(year):
    '''
    queries ACS API for demographic and descriptive data

    Parameters
    ----------
    year : int
        data vintage. will use a five year estimate with most recent year being
        year specified in method parameter

    Returns
    -------
    dataframe containing tabulated ACS data

    '''
    
    #identify list of tables to be pulled
    tables = [
        'B23001',
        'B19013',
        'B17012',
        'B17020',
        'B19001',
        'B19083',
        'B19082',
        'B15003',
        'B15003',
        'B23001',
        'B23001',
        'B28002',
        'B08141',
        'B25077',
        'B25031',
        'B02001',
        'B03001'
        ]
    
    #api endpoint for 2019 acs 5 year
    endpoint = 'https://api.census.gov/data/{}/acs/acs5'.format(year)
    
    #endpoint of variable dictionary
    l = "{}/variables.json".format(endpoint)
    r = requests.get(l)
    j = r.json()
    
    #get all column names of table of interest
    target = dict(filter(lambda item: any([search_key in item[0] for search_key in tables]), j['variables'].items()))
    
    #placeholder dataframe to store api response output
    df = pd.DataFrame(columns = ['state','county','NAME']).set_index(['state','county','NAME'])
    
    #census limits to 50 columns at a time, if we have more than 50 columns we
    #break our workload into chunks
    def roundup(x,i): return int(math.ceil(x / i)) * i
    
    i = 48
    while i <= roundup(len(target),48):
        l = max(0,i-48)
        u = min(i,len(target))
        
        #request current chunk of columns from API
        grps_fmt = ','.join(list(target.keys())[l:u])
        url = '{}?get={},NAME&for=county:*&in=state:08'.format(endpoint,grps_fmt)
        request = requests.get(url)
        
        #transform response into dataframe and join to df
        data = request.json()
        data = pd.DataFrame(data[1:],columns=data[0],dtype='float')
        data.set_index(['state','county','NAME'],inplace=True)
        
        df = df.join(data,how='outer')
        
        #iterate until no more targets remain
        i+=48
    
    #unemployment and labor force
    #identify unemployment and labor force columns
    unemp = [i for i in target if target[i]['label'].endswith('Unemployed')]
    lf = [i for i in target if target[i]['label'].endswith('In labor force:')]
    
    #calculate unemployed and laborforce and then unemployment rate
    df['unemp'] = df[unemp].sum(axis=1)
    df['lf'] = df[lf].sum(axis=1)
    df['UR'] = df['unemp'] / df['lf']
    
    #MHI
    df.rename(columns={'B19013_001E':'MHI'},inplace=True)
    
    #households below poverty line
    df['hh_bpl'] = df['B17012_002E'] / df['B17012_001E']
    
    #poverty by race
    df['white_poverty'] = df['B17020A_002E'] / df['B17020A_001E']
    df['black_poverty'] = df['B17020B_002E'] / df['B17020B_001E']
    df['aian_poverty'] = df['B17020C_002E'] / df['B17020C_001E']
    df['asian_poverty'] = df['B17020D_002E'] / df['B17020D_001E']
    df['nhpi_poverty'] = df['B17020E_002E'] / df['B17020E_001E']
    df['other_poverty'] = df['B17020F_002E'] / df['B17020F_001E']
    df['two_or_more_poverty'] = df['B17020G_002E'] / df['B17020G_001E']
    df['latino_poverty'] = df['B17020I_002E'] / df['B17020I_001E']
    
    #GINI
    df.rename(columns={'B19083_001E':'GINI'},inplace=True)
    
    #top and bottom income quintiles
    df['shr_inc_lq'] = df['B19082_001E'] / 100
    df['shr_inc_hq'] = df['B19082_005E'] / 100
    
    #high school graduation rate
    grads = [i for i in target if 'B15003' in i and int(i[-3:-1]) >= 17]
    df['hs_grad'] = df[grads].sum(axis=1) / df['B15003_001E']
    
    #pct adult pop with bachelors degree
    bac = [i for i in target if 'B15003' in i and int(i[-3:-1]) >= 22]
    df['bachelors'] = df[bac].sum(axis=1) / df['B15003_001E']
    
    #pct adult pop with associates degree
    aso = [i for i in target if 'B15003' in i and int(i[-3:-1]) >= 21]
    df['associates'] = df[aso].sum(axis=1) / df['B15003_001E']
    
    #lf participation rate
    df['LFPR'] = df['lf'] / df['B23001_001E']
    
    #broadband coverage
    df['broadband'] = df['B28002_004E'] / df['B28002_001E']
    
    #people taking public transit
    df['pub_tnst'] = df['B08141_016E'] / df['B08141_001E']
    
    #median home value
    df.rename(columns={'B25077_001E':'med_home_val'},inplace=True)
    
    #median gross rent
    df.rename(columns={'B25031_001E':'med_gross_rent'},inplace=True)
    
    #race
    df['white'] = df['B02001_002E'] / df['B02001_001E']
    df['black'] = df['B02001_003E'] / df['B02001_001E']
    df['aian'] = df['B02001_004E'] / df['B02001_001E']
    df['asian'] = df['B02001_005E'] / df['B02001_001E']
    df['nhpi'] = df['B02001_006E'] / df['B02001_001E']
    df['other'] = df['B02001_007E'] / df['B02001_001E']
    df['hisp_lat'] = df['B03001_003E'] / df['B03001_001E']
    
    df['population'] = df['B02001_001E']
    
    #final product
    df = df[['UR','LFPR','MHI','hh_bpl','white_poverty','black_poverty','aian_poverty',
        'asian_poverty','nhpi_poverty','other_poverty','two_or_more_poverty',
        'latino_poverty','GINI','shr_inc_lq','shr_inc_hq','hs_grad','bachelors','associates',
        'broadband','pub_tnst','med_home_val','med_gross_rent','lf','population',
        'white','black','aian','asian','nhpi','other','hisp_lat']]
    
    df.reset_index(inplace=True)
    df['fips'] = df['state'] * 1000 + df['county']
    df.drop(['county','state'],axis=1,inplace=True)
    df.set_index('fips',inplace=True)
    
    return(df)

def assign_fips(cwdc_etpl):
    '''
    uses fcc geoprocessing api to assign fips codes based on lat and lon

    Parameters
    ----------
    cwdc_etpl : dataframe
        dataframe containing etpl data without fips codes.

    Returns
    -------
    cwdc_etpl

    '''
    
    #api endpoint
    url = 'https://geo.fcc.gov/api/census/area?lat={}&lon={}&format=json'

    #iterate through dataframe to assign fips code for each record
    h = []
    for index, row in cwdc_etpl.iterrows():
        r = requests.get(url.format(row['lat'],row['lon']))
        j = r.json()
        
        h.append({'nid':row['nid'],'fips':j['results'][0]['county_fips']})
    
    etpl_fips = pd.DataFrame(h)
    
    #merge new fips data with old etpl data
    cwdc_etpl = cwdc_etpl.merge(etpl_fips,on='nid')

    return(cwdc_etpl)

def etpl():
    '''
    scrape the dol etpl site to collect etpl data
    CWDC may wish to instead furnish this data themselves, rather than accessing it
    through DOL.

    Returns
    -------
    dataframe containing scraped etpl data

    '''
    
    #headers needed to scrape site
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'cxsearch.dol.gov',
        'Origin': 'https://www.trainingproviderresults.gov',
        'Referer': 'https://www.trainingproviderresults.gov/',
        'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
        }
    
    #look through js to find list of available zip codes
    const = requests.get('https://www.trainingproviderresults.gov/data/constants.js')
    
    zips = [ast.literal_eval(row.strip().strip(',')) for row in const.text.split() if 'zipCode' in row]
    
    #identify which zip codes are within colorado
    r = requests.get('https://api.census.gov/data/2019/acs/acs5?get=NAME,B01001_001E&for=zip%20code%20tabulation%20area:*&in=state:08')
    co_z = pd.DataFrame(r.json()[1:],columns=r.json()[0])
    
    zips = [i for i in zips if i['zipCode'] in list(co_z['zip code tabulation area'])]
    
    #dol search api used by site to serve data
    url = 'https://cxsearch.dol.gov/etp/etp_scorecard_programs/_msearch?source=%7B%7D%0A%7B%22from%22:{},%22size%22:{},%22query%22:%7B%22bool%22:%7B%22must%22:%5B%7B%22bool%22:%7B%22must%22:%5B%7B%22bool%22:%7B%22should%22:%5B%7B%22wildcard%22:%7B%22field_search_api_aggregation_1%22:%22**%22%7D%7D%5D%7D%7D,%7B%22geo_distance%22:%7B%22distance%22:%2225mi%22,%22location%22:%7B%22lat%22:{},%22lon%22:{}%7D%7D%7D,%7B%22terms%22:%7B%22field_program_format%22:%5B%22This+program+provides+online+instruction,+e-learning,+or+distance+learning+only.%22,%22This+program+provides+in-person+instruction+only.%22,%22This+is+a+hybrid+or+blended+program+providing+both+in-person+and+online+instruction.%22%5D%7D%7D%5D%7D%7D%5D%7D%7D,%22sort%22:%7B%22_geo_distance%22:%7B%22location%22:%7B%22lat%22:{},%22lon%22:{}%7D,%22order%22:%22asc%22,%22unit%22:%22mi%22%7D%7D%7D%0A&source_content_type=application%2Fjson'
    
    data = []
    
    #loop through zips using the search api to collect data
    for z in zips:
        low = 0
        inc = 1
        
        r = requests.get(url.format(low,inc,z['latitude'],z['longitude'],z['latitude'],z['longitude']),headers=headers)
        j = r.json()
        
        cap = j['responses'][0]['hits']['total']
        
        inc = 10000
        
        while low <= cap:
            inc = min(inc,cap-low)
            
            r = requests.get(url.format(low,inc,z['latitude'],z['longitude'],z['latitude'],z['longitude']),headers=headers)
            j = r.json()
        
            for hit in j['responses'][0]['hits']['hits']:
                data.append(hit['_source'])
        
            low += 10000
        
        print('{} finished, {} programs'.format(z['zipCode'], cap))
        
    df = pd.DataFrame(data)
    
    #split lat and lon
    df['lat'] = [i['lat'] for i in df['location']]
    df['lon'] = [i['lon'] for i in df['location']]
    df.drop('location',axis=1,inplace=True)
    df.drop_duplicates(inplace=True)
    
    #assign fips codes
    df = assign_fips(df)
    
    #clean completer data
    df['field_c_total_completed'].replace(-1,0,inplace=True)
    
    #create list of in_demand occupations
    socs = in_demand_occupations()    
    
    #count etp by fips code
    prov = pd.DataFrame(df[['fips','field_etp']].drop_duplicates().groupby('fips')['field_etp'].count()).rename(columns={'field_etp':'etp_count'})
    
    #count program and completers by fips code for in_demand related programs
    op_prog = pd.DataFrame(df[df['field_program_soc_occ_1'].str[:-2].isin(socs)].groupby('fips')['nid'].count()).rename(columns={'nid':'etp_in_demand_progs'})
    op_comp = pd.DataFrame(df[df['field_program_soc_occ_1'].str[:-2].isin(socs)].groupby('fips')['field_c_total_completed'].sum()).rename(columns={'field_c_total_completed':'etp_in_demand_completers'})
    
    #repeat for brookings occupations
    b_socs = brookings_occupations()    
        
    b_op_prog = pd.DataFrame(df[df['field_program_soc_occ_1'].str[:-2].isin(b_socs)].groupby('fips')['nid'].count()).rename(columns={'nid':'etp_opportunity_progs'})
    b_op_comp = pd.DataFrame(df[df['field_program_soc_occ_1'].str[:-2].isin(b_socs)].groupby('fips')['field_c_total_completed'].sum()).rename(columns={'field_c_total_completed':'etp_opportunity_completers'})
    
    #join crosstabs together
    out = prov.join([op_prog,op_comp,b_op_prog,b_op_comp]).reset_index().fillna(0)
    
    #drop counties outside the state
    out = out[out['fips'].str.startswith('08')]
    
    out['fips'] = out['fips'].astype(int)
    out.set_index('fips',inplace=True)
        
    return(out)

def cc_data():
    '''
    extract wioa completer data from connecting colorado

    Returns
    -------
    dataframe containing completer and credential data

    '''
    
    #read in data for desired program year
    py19 = pd.read_csv(working_dir+'pirl_py19.csv',header=None)
    
    #placeholder column to correct for data dictionary discrepency
    py19['record_year'] = 19
    data = py19.drop(0,axis=1)
    
    #read in metadata for column names
    meta = pd.read_excel(working_dir+'Master Data Dictionary.xlsx',sheet_name='Sheet2').reset_index()
    meta['index']+=1
    
    #assign column names
    data.columns = list(meta['crec_name'])
    
    #restrict to just colordo residents
    data = data[data['state_code'] == 'CO']
    
    data = data[['uid','county_code','credential_1_type','trained','etp_comp_1']]
    data['fips'] = 8000 + data['county_code']
    
    #calculate the percentage of clients with an occupational cert/licence/credential
    cred = pd.DataFrame(data[data['trained']==1][['uid','fips','credential_1_type']].drop_duplicates().groupby(['fips','credential_1_type'])['uid'].count()).reset_index()
    cred = cred.pivot(index='fips',columns='credential_1_type',values='uid').fillna(0)
    cred['t'] = cred[[i for i in cred if i in range(4,7)]].sum(axis=1)
    cred['credentialed'] = cred['t'] / cred.sum(axis=1)
    
    #calculate the percentage of clients who successfully complete training
    train_comp = pd.DataFrame(data[['uid','fips','etp_comp_1']][data['trained']==1].drop_duplicates().groupby(['fips','etp_comp_1'])['uid'].count()).reset_index()
    train_comp = train_comp.pivot(index='fips',columns='etp_comp_1',values='uid').fillna(0)
    train_comp['train_comp'] = train_comp[1] / train_comp.sum(axis=1)

    #join training completion and credential attainment
    out = cred[['credentialed']].join(train_comp[['train_comp']],how='outer').fillna(0)

    return(out)

def get_qcew():
    '''
    parse qcew annual by area file to colate industry data at the county level

    Returns
    -------
    dataframe containing qcew data at the county level

    '''
    
    #get related industries
    rel_ind = related_industries()
    
    #the qcew data is broken out into individual files for each state and each county
    #loop through the directory to pull only those files containing state and national data
    
    #create an output dataframe to append data to
    qcew = pd.DataFrame()
    
    #directory containing qcew files
    filepath = working_dir+'2019.annual.by_area/'
    
    #loop through the directory
    for filename in os.listdir(filepath):
        if 'Colorado' in filename:
            #if the file is a state or national file, skip it
            if not any(x in filename for x in ['-- Statewide','U.S. TOTAL']):
                #read file to temporary dataframe
                temp = pd.read_csv(filepath+filename)
                temp = temp[temp['industry_code'].isin(rel_ind + ['44-45','71','72'])]
                #rename columns to match database names
                temp.rename(columns={'area_fips':'fips'},inplace=True)
                
                #append to output dataframe
                qcew = qcew.append(temp[['fips','industry_code','annual_avg_estabs_count','annual_avg_emplvl','avg_annual_pay',
                                         'oty_annual_avg_estabs_count_chg']])

    #format and transform
    qcew = qcew[[str(qcew.loc[i,'fips']).startswith('8') for i in qcew.index]]
    qcew['fips'] = qcew['fips'].astype(int)

    qcew['group'] = ['ret_accom' if i in ['44-45','71','72'] else 'rel_ind' for i in qcew['industry_code']]

    #calculate retail and accomodation figures
    qcew_ret_accom = qcew[qcew['group']=='ret_accom'].groupby('fips').agg({
        'annual_avg_estabs_count':'sum',
        'annual_avg_emplvl':'sum',
        'avg_annual_pay':'mean',
        'oty_annual_avg_estabs_count_chg':'sum'
        }).rename(columns={
            'annual_avg_estabs_count':'ret_accom_annual_avg_estabs_count',
            'annual_avg_emplvl':'ret_accom_annual_avg_emplvl',
            'avg_annual_pay':'ret_accom_avg_annual_pay',
            'oty_annual_avg_estabs_count_chg':'ret_accom_oty_annual_avg_estabs_count_chg'
            })
    
    #calculate related industry figures
    qcew_rel_ind = qcew[qcew['group']!='ret_accom'].groupby('fips').agg({
        'annual_avg_estabs_count':'sum',
        'annual_avg_emplvl':'sum',
        'avg_annual_pay':'mean',
        'oty_annual_avg_estabs_count_chg':'sum'
        }).rename(columns={
            'annual_avg_estabs_count':'rel_ind_annual_avg_estabs_count',
            'annual_avg_emplvl':'rel_ind_annual_avg_emplvl',
            'avg_annual_pay':'rel_ind_avg_annual_pay',
            'oty_annual_avg_estabs_count_chg':'rel_ind_oty_annual_avg_estabs_count_chg'
            })

    qcew = qcew_ret_accom.join(qcew_rel_ind,how='outer')
    
    #generate rate figures
    qcew['ret_accom_estab_chg_rt'] = qcew['ret_accom_oty_annual_avg_estabs_count_chg'] / qcew['ret_accom_annual_avg_estabs_count']
    qcew['rel_ind_estab_chg_rt'] = qcew['rel_ind_oty_annual_avg_estabs_count_chg'] / qcew['rel_ind_annual_avg_estabs_count']

    qcew.drop(['ret_accom_oty_annual_avg_estabs_count_chg','rel_ind_oty_annual_avg_estabs_count_chg'],axis=1,inplace=True)
    
    return(qcew)

def crime_data():
    '''
    FBI crime data tabulated at the county area

    Returns
    -------
    dataframe containing tabulated crime data

    '''
    
    #read in incident and agency data
    inc = pd.read_csv(working_dir+'CO-2019/CO/NIBRS_incident.csv')
    agencies = pd.read_csv(working_dir+'CO-2019/CO/agencies.csv')
    
    #iterate over agencies dataframe, collect agency id, name, and county
    agc = []
    for index, row in agencies.iterrows():
        if str(row['COUNTY_NAME']) == 'nan':
            continue
        
        cos = row['COUNTY_NAME'].split('; ')
        for co in cos:
            agc.append({'AGENCY_ID':row['AGENCY_ID'],'NAME':'{} County, Colorado'.format(co.lower().title())})

    agc = pd.DataFrame(agc)
    
    #calculate crime incident multiplier for agencies that stretch across multiple counties
    agc['crime_incidents'] = 1 / agc['NAME'].groupby(agc['AGENCY_ID']).transform('count')

    #merge with incident record
    inc = inc.merge(agc,on='AGENCY_ID')
    
    #aggregate to county level
    crime = pd.DataFrame(inc.groupby('NAME')['crime_incidents'].sum()).reset_index()

    return(crime)

def get_emsi_ind(filepath):
    '''
    process emsi industry data

    Parameters
    ----------
    filepath : str
        path to folder containing emsi industry data

    Returns
    -------
    dataframe containing tabulated emsi industry data

    '''
    
    #emty dataframe to receive emsi data
    data = pd.DataFrame()
    
    #columns to rename
    cols = {
        'Total Diversity % of Industry':'div_emp_per',
        }
    
    #loop through files in target folder
    for filename in os.listdir(filepath):
        #read in data
        temp = pd.read_excel(filepath+filename,sheet_name='Industries')
        
        #identify most recent year, oldest year, and second most recent year
        low = list(temp.columns)[2]
        mid = list(temp.columns)[-3]
        high = list(temp.columns)[-2]
        
        #identify county
        fips = pd.read_excel(filepath+filename,sheet_name='Parameters').iloc[5,0]
        
        #clean data
        temp.replace('Insf. Data',np.nan,inplace=True)
        temp.replace('<10',0,inplace=True)
        temp.fillna(0,inplace=True)
        temp.rename(columns=cols,inplace=True)
        temp['fips'] = int(fips)
        
        #calculate percent change values
        temp['ind_per_chng_1'] = (temp[high]-temp[mid]) / temp[mid]
        temp['ind_per_chng_5'] = (temp[high]-temp[low]) / temp[low]
        
        #append to holder dataframe
        data = data.append(temp[['NAICS','Description','fips','div_emp_per','ind_per_chng_1','ind_per_chng_5']])
        
    #clean inf and -inf
    data.replace([np.inf,-np.inf], np.nan, inplace=True)
    
    #create list of related industries
    rel_ind = related_industries()
    
    ret_accom_ind = ['44','45','71','72']
        
    #identify industry type
    data['ret_accom_ind'] = [any(str(i).startswith(x) for x in ret_accom_ind) for i in data['NAICS']]
    data['rel_ind'] = [str(i).split('.')[0] in rel_ind for i in data['NAICS']]
    
    data['ind_type'] = np.nan
    data.loc[data['ret_accom_ind'], 'ind_type'] = 'ret_accom_ind'
    data.loc[data['rel_ind'], 'ind_type'] = 'rel_ind'
    
    #aggregate to fips and industry level
    emsi_ind = data.groupby(['fips','ind_type']).agg({
        'div_emp_per':'mean', 
        'ind_per_chng_1':'mean',
        'ind_per_chng_5':'mean'
        }).reset_index()
    
    #reshape into a wide dataframe for use in score
    target = emsi_ind[emsi_ind['ind_type']=='ret_accom_ind'].drop('ind_type',axis=1)
    target.set_index('fips',inplace=True)
    target.columns = ['ret_accom_ind_{}'.format(i) for i in target] 
    
    rel = emsi_ind[emsi_ind['ind_type']!='ret_accom_ind'].drop('ind_type',axis=1)
    rel.set_index('fips',inplace=True)
    rel.columns = ['rel_ind_{}'.format(i) for i in rel] 
    
    emsi_ind = target.join(rel)
    
    return(emsi_ind)

def get_emsi_soc(filepath):
    '''
    process emsi occupation data

    Parameters
    ----------
    filepath : str
        path to folder containing emsi occupation data

    Returns
    -------
    dataframe containing tabulated emsi occupation data

    '''
    
    #emty dataframe to receive emsi data
    data = pd.DataFrame()
    
    #columns to rename
    cols = {
        'Avg. Annual Openings':'occ_openings', 
        'Pct. 25 Annual Earnings':'occ_per_25_earn',
        'Total Diversity % of Occupation':'occ_div_emp_per',
        'COL Index':'coli',
        'Automation Index':'auto',
        '2020 Resident Workers':'res_work'
        }
    
    #loop through the files in the folder
    for filename in os.listdir(filepath):
        #read in the data and add county name based on file name
        temp = pd.read_csv(filepath+filename)
        temp['NAME'] = filename.split('_in_')[1].split('_CO_')[0].replace('_',' ') + ', Colorado'
        
        #clean data
        temp.replace('Insf. Data',np.nan,inplace=True)
        temp.rename(columns=cols,inplace=True)
        
        #append to dataframe
        data = data.append(temp)

    #read in front line occupations list
    cwdc_socs = pd.read_csv(working_dir+'cwdc_socs.txt',sep='|',header=None)
    cwdc_socs = [i.split()[0] for i in cwdc_socs[0]]
    
    #calculate automation index scores weighted by local employment levels
    auto = data[data['SOC'].isin(cwdc_socs)]
    auto['auto'] = (auto['auto'] * auto['res_work']) / auto['res_work'].groupby(auto['NAME']).transform('sum')

    soc_11 = data[data['SOC'].str.startswith('11')]

    #create in_demand and brookings occupations lists
    socs = in_demand_occupations()
    b_socs = brookings_occupations()
    
    op_data = data[data['SOC'].isin(socs)]
    b_data = data[data['SOC'].isin(b_socs)] 
    
    #calculate in_demand occupations statistics by county
    emsi_soc = op_data.groupby('NAME').agg({
        'occ_openings':'mean',
        'occ_per_25_earn':'median',
        'occ_div_emp_per':'mean', 
        'coli':'mean'
        }).reset_index()
    
    #calculate brookings occupations statistics by county
    brookings_soc = b_data.groupby('NAME').agg({
        'occ_openings':'mean',
        'occ_per_25_earn':'median',
        'occ_div_emp_per':'mean', 
        'coli':'mean'
        }).reset_index().rename(columns={
            'occ_openings':'opportunity_occ_openings',
            'occ_per_25_earn':'opportunity_occ_per_25_earn',
            'occ_div_emp_per':'opportunity_occ_div_emp_per', 
            'coli':'opportunity_coli'
            })
    
    #calculate employment diversity of management occupations by county
    emsi_soc_11 = soc_11.groupby('NAME').agg({
        'occ_div_emp_per':'mean'
        }).reset_index().rename(columns={
        'occ_div_emp_per':'management_div_emp_per'
        })
            
    #calculate automation index by county
    emsi_auto = auto.groupby('NAME').agg({
        'auto':'sum'
        }).reset_index()
    
    #merge data together
    emsi_soc = emsi_soc.merge(brookings_soc,on='NAME')
    emsi_soc = emsi_soc.merge(emsi_soc_11,on='NAME')
    emsi_soc = emsi_soc.merge(emsi_auto,on='NAME')
    
    return(emsi_soc)

    
def get_census():
    '''
    tabulate census participation data

    Returns
    -------
    dataframe of census participaitno data

    '''
    
    #read in and rename colums from census participatin rate file
    census = pd.read_excel(working_dir+'CO Census Participation Rates 2010.xlsx').rename(
        columns={'UniqueID':'fips','Participation Rate (2010)':'part_rate'}).set_index('fips')
    return(census[['part_rate']])

def get_regions():
    '''
    scrape colorado oedit economic areas

    Returns
    -------
    dataframe of region definitions

    '''
    
    #soup the html of the oedit regions page
    r = requests.get('https://choosecolorado.com/doing-business/regions/')
    soup = bs(r.content)
    
    #identify elements containing region descriptions
    leeds = [i for i in soup.findAll('div',{'class':'wrap'}) if 'Region ' in i.text]
    
    #loop through each region to identify each county within it
    regions = []
    for l in leeds:
        region = l.find('div',{'class':'region-title'}).text.strip('\n').strip()
        counties = l.find('div',{'class':'counties'}).text.strip('\n').strip().replace(' and ', ', ').split(', ')
        counties = ['{} County'.format(i) for i in counties]
        
        for county in counties:
            regions.append({'region':region,'County':county})
        
    regions = pd.DataFrame(regions)
        
    #retrieve list of colorado counties
    r = requests.get('https://en.wikipedia.org/wiki/List_of_counties_in_Colorado')
    soup = bs(r.content)

    leeds = [i for i in soup.findAll('table')]
    
    counties = []
    
    for row in leeds[0].findAll('tr')[1:]:
        counties.append({'County':row.find('th').text.strip('\n'),
                         'fips':'08{}'.format(row.find('td').text.strip('\n'))})
        
    counties = pd.DataFrame(counties)
    counties['County'] = [i.replace('City and County of ','') + ' County' if 'City' in i else i
                          for i in counties['County']]

    regions['County'] = [i.replace(',','') for i in regions['County']]

    #merge regions with counties to assign fips codes
    regions = regions.merge(counties,how='outer')
    regions['fips'] = regions['fips'].astype(int)
    regions.set_index('fips',inplace=True)
    regions.drop('County',axis=1,inplace=True)

    return(regions)

def normalize(data):
    '''
    normalizes data in columns of a data frame

    Parameters
    ----------
    data : dataframe
        data to be normalized

    Returns
    -------
    dataframe of normalized data

    '''
    
    #make local copy of data
    df = data.copy()
    
    #loop through columns to normalize
    for c in df:
        df[c] = (df[c] - df[c].min()) / (df[c].max() - df[c].min())

    return(df)

def normative_score(score):
    '''
    create a simplified, qualitative score description for input scores

    Parameters
    ----------
    score : dataframe
        dataframe containing final scores to be simplified.

    Returns
    -------
    None.

    '''
    
    def simplify(series):
        '''
        method to simplify input series

        Parameters
        ----------
        series : series
            column from input dataframe.

        Returns
        -------
        None.

        '''
        
        #calculate z statistic
        z = ( series - series.mean() ) / series.std()
       
        #relable series contents based on z score
        series.loc[z >= 1] = 'high'
        series.loc[(1 > z) & (z >= -1)] = 'average'
        series.loc[-1 > z] = 'low'
    
    #make local copy of score dataframe
    data = score.copy()
    
    #apply simplify method
    data.apply(lambda col: simplify(col))
    
    return(data)

def to_file(path,score,simple_score,data_raw,data_norm):
    '''
    method to write final score tables and input data to a single excel file

    Parameters
    ----------
    path : str
        path to output file.
    score : dataframe
        dataframe containing final scores as floats.
    simple_score : dataframe
        dataframe containing simplified scores as strings ("high", "average", "low").
    data_raw : dataframe
        dataframe containing raw input data used to construct the scores.
    data_norm : dataframe
        dataframe containing normalized data used to construct the scores.

    Returns
    -------
    None.

    '''
    sv_dict = {
        'etp_in_demand_progs':'education_training',
        'in_demand_programs':'education_training',
        'etp_opportunity_progs':'education_training',
        'opporunity_programs':'education_training',
        'hh_bpl':'regional_context',
        'MHI':'regional_context',
        'etp_in_demand_completers':'regional_job_opportunities',
        'etp_opportunity_completers':'regional_job_opportunities',
        'occ_openings':'regional_job_opportunities', 
        'occ_per_25_earn':'regional_job_opportunities', 
        'occ_div_emp_per':'regional_job_opportunities',
        'opportunity_occ_openings':'regional_job_opportunities', 
        'opportunity_occ_per_25_earn':'regional_job_opportunities', 
        'opportunity_occ_div_emp_per':'regional_job_opportunities',
        'auto':'regional_job_opportunities',
        'hs_grad':'individual',
        'credentialed':'individual',
        'train_comp':'individual',
        'ret_accom_ind_div_emp_per':'industry', 
        'ret_accom_ind_ind_per_chng_1':'industry',
        'ret_accom_ind_ind_per_chng_5':'industry', 
        'rel_ind_div_emp_per':'industry',
        'rel_ind_ind_per_chng_1':'industry', 
        'rel_ind_ind_per_chng_5':'industry',
        'ret_accom_annual_avg_emplvl':'industry',
        'rel_ind_annual_avg_emplvl':'industry',
        'ret_accom_avg_annual_pay':'industry',
        'rel_ind_avg_annual_pay':'industry',
        'bachelors':'neighborhood',
        'coli':'neighborhood',
        'absentee_rt':'neighborhood',
        'crime_incidents':'neighborhood',
        'UR':'neighborhood',
        'LFPR':'neighborhood',
        'part_rate':'engagement',
        'management_div_emp_per':'engagement',
        'sector_strat':'engagement',
        'cwdc_response':'engagement',
        'county_nonwhite':'not directly part of a score', 
        'population':'not directly part of a score', 
        'lf':'not directly part of a score', 
        'region_nonwhite':'not directly part of a score'
        }

    name_dict = {
        'hs_grad':'population with a high school diploma or equivalent, percent',
        'credentialed':'connecting colorado population with an occupational certification, license, or certificate, percent', 
        'train_comp':'connecting colorado population completing training, percent', 
        'bachelors':'population with a bachelors degree or higher, percent', 
        'coli':'cost of living index',
        'absentee_rt':'primary and secondary students experiencing chronic absenteeism, percent', 
        'crime_incidents':'crimes per capita', 
        'UR':'unemployment rate, economic area',
        'LFPR':'labor force participation rate, economic area',
        'ret_accom_ind_div_emp_per':'retail, accomodation, food service, arts, entertainment, and recreation diversity relative to regional diversity, ratio', 
        'ret_accom_ind_ind_per_chng_1':'retail, accomodation, food service, arts, entertainment, and recreation 1 year employment percent change, percent',
        'ret_accom_ind_ind_per_chng_5':'retail, accomodation, food service, arts, entertainment, and recreation 5 year employment percent change, percent', 
        'rel_ind_div_emp_per':'related industries diversity relative to regional diversity, ratio',
        'rel_ind_ind_per_chng_1':'related industries 1 year employment percent change, percent', 
        'rel_ind_ind_per_chng_5':'related industries 5 year employment percent change, percent',
        'ret_accom_annual_avg_emplvl':'retail, accomodation, food service, arts, entertainment, and recreation employment, per individuals in labor force',
        'rel_ind_annual_avg_emplvl':'related industries employment, per individuals in labor force',
        'ret_accom_avg_annual_pay':'retail, accomodation, food service, arts, entertainment, and recreation average annual pay, relative to area median household income',
        'rel_ind_avg_annual_pay':'related industries annual pay, relative to area median household income',
        'part_rate':'2010 Census participation rate, percent',
        'management_div_emp_per':'management occupations diversity relative to regional diversity, ratio', 
        'county_nonwhite':'nonwhite county population, percent', 
        'sector_strat':'presence of sector strategy [ CURRENTLY BLANK ]',
        'cwdc_response':'response rate to CWDC survey [CURRENTLY BLANK ]', 
        'hh_bpl':'households below poverty line, percent', 
        'MHI':'median household income, dollars', 
        'etp_in_demand_progs':'etp in demand programs per capita',
        'etp_in_demand_completers':'etp in demand program completers per capita', 
        'in_demand_programs':'ipeds in demand programs per capita',
        'etp_opportunity_progs':'etp opportunity programs per capita', 
        'etp_opportunity_completers':'etp opportunity program completers per capita',
        'opporunity_programs':'ipeds opportunity programs per capita',
        'population':'population count', 
        'occ_openings':'in demand occupational openings', 
        'occ_per_25_earn':'in demand occupations median pay 25th percentile, dollars',
        'occ_div_emp_per':'in demand occupations diversity relative to area diversity, ratio', 
        'opportunity_occ_openings':'opportunity occupational openings',
        'opportunity_occ_per_25_earn':'opportunity occupations median pay 25th percentile, dollars', 
        'opportunity_occ_div_emp_per':'opportunity occupations diversity relative to area diversity, ratio', 
        'lf':'labor force size',
        'region_nonwhite':'nonwhite regional population, percent', 
        'auto':'local automation index risk for frontline occupations'
        }

    key_list = list(name_dict.keys())
    val_list = list(name_dict.values())
    
    data = data_raw.copy()
    norm = data_norm.copy()

    data['hh_bpl'] = 1 - data['hh_bpl']
    data['UR'] = 1 - data['UR']
    data['absentee_rt'] = 1 - data['absentee_rt']

    counties = list(data.index)
    
    data.rename(columns=name_dict,inplace=True)
    norm.rename(columns=name_dict,inplace=True)

    avg = pd.DataFrame(data.mean()).rename(columns={0:'raw_data_state_average'})
    
    with pd.ExcelWriter(path, engine = 'openpyxl', mode = 'a') as writer:
        simple_score.to_excel(writer,sheet_name='simple_score')
        
        for co in counties:
            dat = pd.DataFrame(data.loc[co])
            dat.columns = ['raw_data']
            
            nv = pd.DataFrame(norm.loc[co])
            nv.columns = ['normalized_data']
            
            sv = pd.DataFrame(simple_score.loc[co[0]]).T.reset_index()
            sv.columns = ['category','simplified_category_score']
            sv = sv.append(pd.DataFrame([['not directly part of a score',np.nan]],columns=['category','simplified_category_score']))
            
            s = pd.DataFrame(score.loc[co[0]]).T.reset_index()
            s.columns = ['category','category_score']
            s = s.append(pd.DataFrame([['not directly part of a score',np.nan]],columns=['category','category_score']))
            
            sv = s.merge(sv,on='category')
            
            df = dat.join([avg,nv],how='outer')
            df['category'] = [sv_dict[key_list[val_list.index(i)]] for i in df.index]
            
            df.reset_index(inplace=True)
            df.rename(columns={'index':'indicator'},inplace=True)
            df.set_index(['category','indicator'],inplace=True)
            
            df = df.reset_index().merge(sv,on='category',how='outer').set_index(['category', 'indicator'])
            
            df.to_excel(writer,sheet_name=co[1].replace(', Colorado','').replace(' ','_').lower())



def score():
    #generate input data
    cwdc_ipeds = ipeds(2018)
    cwdc_acs = acs(2019)
    cwdc_etpl = etpl()
    cc = cc_data()
    qcew = get_qcew()
    crime = crime_data()
    emsi_ind = get_emsi_ind(working_dir+'emsi_ind_co/')
    emsi_soc = get_emsi_soc(working_dir+'emsi_occ_co/')
    census = get_census()

    #join and merge input data together
    c_idx = cwdc_acs.join([cwdc_ipeds,cwdc_etpl,cc,qcew,emsi_ind,census],how='outer')
    c_idx.reset_index(inplace=True)
    c_idx = c_idx.merge(emsi_soc,on='NAME')    
    c_idx = c_idx.merge(crime,on='NAME',how='left')
    c_idx.set_index('fips',inplace=True)
    
    c_idx = c_idx[~c_idx['NAME'].isna()]
    
    #correct relationship directions    
    c_idx['hh_bpl'] = 1 - c_idx['hh_bpl']
    c_idx['GINI'] = 1 - c_idx['GINI']
    c_idx['UR'] = 1 - c_idx['UR']
    c_idx['absentee_rt'] = 1 - c_idx['absentee_rt']

    #local dataset
    #normalize certain variables to labor force, median household income, and population
    c_idx['ret_accom_annual_avg_estabs_count'] /= c_idx['lf']
    c_idx['rel_ind_annual_avg_estabs_count'] /= c_idx['lf']
    c_idx['ret_accom_annual_avg_emplvl'] /= c_idx['lf']
    c_idx['rel_ind_annual_avg_emplvl'] /= c_idx['lf']
    
    c_idx['ret_accom_avg_annual_pay'] /= c_idx['MHI']
    c_idx['rel_ind_avg_annual_pay'] /= c_idx['MHI']
    c_idx['single_housing_cost'] /= c_idx['MHI']
    c_idx['single_childcare_cost'] /= c_idx['MHI']
    c_idx['single_food_cost'] /= c_idx['MHI']
    c_idx['single_transportation_cost'] /= c_idx['MHI']
    c_idx['family_housing_cost'] /= c_idx['MHI']
    c_idx['family_childcare_cost'] /= c_idx['MHI']
    c_idx['family_food_cost'] /= c_idx['MHI']
    c_idx['family_transportation_cost'] /= c_idx['MHI']

    c_idx['crime_incidents'] /= c_idx['population']
    
    #calculate nonwhite percentage
    c_idx['nonwhite'] = 1 - c_idx['white']

    #placeholder, will ideally be replaced by cwdc survey data
    c_idx['cwdc_response'] = np.nan
    c_idx['sector_strat'] = np.nan

    #regional dataset
    
    #create region list and join to index data
    regions = get_regions()
    c_idx = c_idx.join(regions)
    
    #aggregate regional data
    reg_data = c_idx.groupby('region').agg({
        'hh_bpl':'mean',
        'MHI':'median',
        'etp_in_demand_progs':'sum',
        'etp_in_demand_completers':'sum',
        'in_demand_programs':'sum',
        'etp_opportunity_progs':'sum',
        'etp_opportunity_completers':'sum',
        'opporunity_programs':'sum',
        'population':'sum',
        'occ_openings':'sum', 
        'occ_per_25_earn':'median', 
        'occ_div_emp_per':'mean',
        'opportunity_occ_openings':'sum', 
        'opportunity_occ_per_25_earn':'median', 
        'opportunity_occ_div_emp_per':'mean',
        'lf':'sum',
        'nonwhite':'mean',
        'auto':'mean'
        })
    
    #adjust education and job variables
    reg_data['etp_in_demand_progs'] /= reg_data['population']
    reg_data['etp_in_demand_completers'] /= reg_data['population']
    reg_data['etp_opportunity_progs'] /= reg_data['population']
    reg_data['etp_opportunity_completers'] /= reg_data['population']
    reg_data['in_demand_programs'] /= reg_data['population']
    reg_data['opporunity_programs'] /= reg_data['population']
    reg_data['occ_openings'] /= reg_data['lf']
    reg_data['occ_per_25_earn'] /= reg_data['MHI']
    reg_data['occ_div_emp_per'] /= reg_data['nonwhite']
    reg_data['opportunity_occ_openings'] /= reg_data['lf']
    reg_data['opportunity_occ_per_25_earn'] /= reg_data['MHI']
    reg_data['opportunity_occ_div_emp_per'] /= reg_data['nonwhite']
    
    reg_data.replace([np.inf,-np.inf], np.nan, inplace=True)
    
    #write regional data to file
    reg_data.to_csv(working_dir+'cwdc_regional_data.csv')
    
    #normalize regional data
    reg_norm = normalize(reg_data)
    
    #reverse normalized automation score to make larger numbers corespond to better outcomes
    reg_norm['auto'] = 1 - reg_norm['auto']
                       
    #calculate education and training category
    ed = pd.DataFrame(reg_norm[[
        'etp_in_demand_progs',
        'in_demand_programs',
        'etp_opportunity_progs',
        'opporunity_programs',
        ]].mean(axis=1)).rename(columns={0:'education_training'})
                       
    #calculate regional context category
    reg = pd.DataFrame(reg_norm[[
        'hh_bpl',
        'MHI'
        ]].mean(axis=1)).rename(columns={0:'regional_context'})

    #calculate regional job opportunities category
    occ = pd.DataFrame(reg_norm[[
        'etp_in_demand_completers',
        'etp_opportunity_completers',
        'occ_openings', 
        'occ_per_25_earn', 
        'occ_div_emp_per',
        'opportunity_occ_openings', 
        'opportunity_occ_per_25_earn', 
        'opportunity_occ_div_emp_per',
        'auto'
        ]].mean(axis=1)).rename(columns={0:'regional_job_opportunities'})

    #join regional categories together
    reg = reg.join([ed,occ],how='outer').reset_index()

    #local data input variables
    cols = [
        'hs_grad',
        'credentialed',
        'train_comp',
        'bachelors',
        'coli',
        'absentee_rt',
        'crime_incidents',
        'UR',
        'LFPR',
        'ret_accom_ind_div_emp_per', 
        'ret_accom_ind_ind_per_chng_1',
        'ret_accom_ind_ind_per_chng_5', 
        'rel_ind_div_emp_per',
        'rel_ind_ind_per_chng_1', 
        'rel_ind_ind_per_chng_5',
        'ret_accom_annual_avg_emplvl',
        'rel_ind_annual_avg_emplvl',
        'ret_accom_avg_annual_pay',
        'rel_ind_avg_annual_pay',
        'part_rate',
        'management_div_emp_per',
        'nonwhite',
        'sector_strat',
        'cwdc_response'
        ]
    
    local_data = c_idx[cols]
    
    #calculate ratio of management diversity to local nonwhite population
    local_data['management_div_emp_per'] /= local_data['nonwhite']
    
    #normalize local data
    local_norm = normalize(local_data.fillna(0))
    
    #adjust coli so that a higher score contributes to a lower overall score
    local_norm['coli'] = 1 - local_norm['coli']
    
    #caluclate individual endowments category
    indiv = pd.DataFrame(local_norm[[
        'hs_grad',
        'credentialed',
        'train_comp'
        ]].mean(axis=1)).rename(columns={0:'individual'})
    
    #calculate industry employment category
    industry = pd.DataFrame(local_norm[[
        'ret_accom_ind_div_emp_per', 
        'ret_accom_ind_ind_per_chng_1',
        'ret_accom_ind_ind_per_chng_5', 
        'rel_ind_div_emp_per',
        'rel_ind_ind_per_chng_1', 
        'rel_ind_ind_per_chng_5',
        'ret_accom_annual_avg_emplvl',
        'rel_ind_annual_avg_emplvl',
        'ret_accom_avg_annual_pay',
        'rel_ind_avg_annual_pay'
        ]].mean(axis=1)).rename(columns={0:'industry'})
    
    #calculate neighborhood context category
    nbhd = pd.DataFrame(local_norm[[
        'bachelors',
        'coli',
        'absentee_rt',
        'crime_incidents',
        'UR',
        'LFPR'
        ]].mean(axis=1)).rename(columns={0:'neighborhood'})

    #calculate leadership and engagement category
    engage = pd.DataFrame(local_norm[[
        'part_rate',
        'management_div_emp_per',
        'sector_strat',
        'cwdc_response'
        ]].mean(axis=1)).rename(columns={0:'engagement'})

    #join local data
    local = indiv.join([industry,nbhd,engage])
    
    #combine local and regional data into a final score datafrme
    local = local.join(c_idx[['NAME','region']])
    score = local.reset_index().merge(reg,on='region',how='outer').drop('region',axis=1)
    score.rename(columns={'index':'fips'},inplace=True)
    score.set_index(['fips','NAME'],inplace=True)
    
    #calculate the combined score
    score['combined_score'] = score.mean(axis=1)
    
    #generate simple scores
    simple_score = normative_score(score)    
    
    #write score and simple score to file
    score.to_csv(working_dir+'cwdc_index_scores.csv')
    simple_score.to_csv(working_dir+'cwdc_index_scores_simple.csv')
    
    #construct normalized values dataframe from both local and regional data
    local_norm = local_norm.join(c_idx[['NAME','region']])
    norm = local_norm.reset_index().merge(reg_norm.reset_index(),on='region',how='outer').drop('region',axis=1)
    norm.rename(columns={'index':'fips'},inplace=True)
    norm.set_index(['fips','NAME'],inplace=True)
    
    #generate simplified normalized values
    simple_norm = normative_score(norm)
    
    #write normalized values to file
    norm.to_csv(working_dir+'cwdc_index_normalized_values.csv')
    simple_norm.to_csv(working_dir+'cwdc_index_normalized_values_simple.csv')

    #construct statewide averages and add to index data
    agg = pd.DataFrame(c_idx.mean()).T
    agg['fips'] = 8999
    agg['NAME'] = 'statewide average'
    agg.set_index(['fips','NAME'],inplace=True)

    c_idx.reset_index(inplace=True)
    c_idx.rename(columns={'index':'fips'},inplace=True)
    c_idx.set_index(['fips','NAME'],inplace=True)
    
    c_idx = c_idx.append(agg)
    c_idx.reset_index(inplace=True)
    c_idx.set_index(['fips','NAME','region'],inplace=True)
    
    #write to file
    c_idx.to_csv(working_dir+'cwdc_index_data.csv')
    
    
    #write to file
    temp = c_idx.reset_index()[['fips','NAME','region']]
    local_data = local_data.join(temp.set_index('fips'))
    #local_norm = local_norm.join(regions)
    
    master_data = local_data.reset_index().merge(reg_data.reset_index(),on='region').set_index(['fips', 'NAME', 'region'])
    for c in master_data:
        if c.endswith('_x'):
            master_data.rename(columns={c:'county_'+c.strip('_x')},inplace=True)
        if c.endswith('_y'):
            master_data.rename(columns={c:'region_'+c.strip('_y')},inplace=True)
    
    master_norm = local_norm.reset_index().merge(reg_norm.reset_index(),on='region').set_index(['fips', 'NAME', 'region'])
    for c in master_norm:
        if c.endswith('_x'):
            master_norm.rename(columns={c:'county_'+c.strip('_x')},inplace=True)
        if c.endswith('_y'):
            master_norm.rename(columns={c:'region_'+c.strip('_y')},inplace=True)

    with pd.ExcelWriter(working_dir + 'cwdc_county_summaries.xlsx', engine = 'openpyxl', mode = 'w') as writer:
        score.to_excel(writer,sheet_name='scores')
        writer.save()

    to_file(working_dir + 'cwdc_county_summaries.xlsx',
            score.copy(),
            simple_score.copy(),
            master_data.copy(),
            master_norm.copy())    

if __name__ == '__main__':
    #location of index data
    working_dir = 'FILE PATH TO INPUT DATA LOCATION'
    
    score()