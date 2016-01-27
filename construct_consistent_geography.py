##############################
# NAME: construct_consistent_geography.py
# PURPOSE: construct consistent sets of census blocks over different
#		   combinations of years. A consistent census block is a set of census
#		   blocks for each year such that the no block within the given
#	       consistent census block changes consistent census blocks over time
#		   the relevant time period (i.e. 1990, 2000 and 2010)
# INPUTS: a) Census tabulation block relationship files for 1990 to 2000 and 
#		     2000 to 2010.
#	      b) State county, state name, and statefips dictionaries.
#
# LAST EDITED: 1/14/2016
# EDITED BY: Alex Bartik
# ORIGINAL BY: Alex Bartik
###############################

import os, datetime, csv
import cPickle as pickle 
import networkx as nx
import numpy as np
import pandas as pd 

t0 = datetime.datetime.now()


# MAKING A SMALL CHANGE.  IS THIS ON THE BRANCH?

##############################
# 0.1: Defining directories
GENT = 'C:/Users/Bartik/Dropbox/Research/Gentrification/'
CODE = GENT + '/Data/Code/'
SOURCE_GEOGRAPHIC = GENT + '/Data/Raw/Geographic/Source Data/'
MODIFIED_GEOGRAPHIC = GENT + '/Data/Raw/Geographic/Modified Data/'
TEMP = GENT + '/Data/Temp/'
LOG = GENT + '/Results/Log/'

##############################
# 0.2: Settings

infolder = SOURCE_GEOGRAPHIC
outfolder = MODIFIED_GEOGRAPHIC

input_states = MODIFIED_GEOGRAPHIC + 'state_list.csv'
statesall = pd.read_csv(input_states)
statelist = list(statesall.state_postal)

#################################
# 0.3: Loading dictionaries 
with open(MODIFIED_GEOGRAPHIC + '//' + 'stateCounty_dict.p', 'rb') as fp:
	stateCounty_dict = pickle.load(fp)   
 
with open(MODIFIED_GEOGRAPHIC + '//' + 'dict_state_names.p', 'rb') as fp:
	dict_state_names = pickle.load(fp)   
 
with open(MODIFIED_GEOGRAPHIC + '//' + 'dict_state_codes.p', 'rb') as fp:
	dict_state_codes_reverse = pickle.load(fp)

dict_state_codes = dict((v,k) for k,v in dict_state_codes_reverse.iteritems())

#################################
# 0.4: defining programs used during code

def connected_sets(inlist) :
    subset = inlist[['blk1','blk2']]
    tuples = [tuple(x) for x in subset.values]
    G = nx.Graph(tuples)
    return sorted(nx.connected_components(G), key = len, reverse = True)
    del G, subset, tuples


def make_table(inconnect,groupidname):
    table = [['blkid',groupidname]]
    table0 = ['blkid']
    for i in range(len(inconnect)):
        if i%10000==0 :
            print i
    
        for j in inconnect[i]:
            table.append([j,i])
            table0.append(j)
      
    assert len(table0) == len(set(table0))      
    del table0
    
    return table
 	
##############################

for state in statelist :
    t1 = datetime.datetime.now()
    
    state_name = dict_state_names[dict_state_codes[state]]
    print 'Welcome to beautiful: ' + state_name

##### 1) inputting 1990 to 2000 relationship files
    state_str = dict_state_codes[state].zfill(2)    
    input_vars90 = ['state90','cty90','tract90','blk90','part90','state00','cty00','tract00','blk00','part00']

    for county in stateCounty_dict[state]:
        print 'County ' + county 
        fileinput90 = infolder + 't9t2' + county + '.txt'
        
        if os.path.isfile(fileinput90):

            filecty = pd.read_csv(fileinput90,names = input_vars90,dtype = str)
            
            if stateCounty_dict[state].index(county) == 0 :
                file90 = filecty
            else :
                file90 = file90.append(filecty)
                print type(file90)
                print len(file90)
                print list(file90.columns.values)
        
        elif not os.path.isfile(fileinput90):
            print 'The 1990 - 2000 relationship for ' + county + ' does not exist'
            
    
    file90['blk90'] = file90['blk90'].apply(lambda x: x.zfill(4))    
    file90['blk1990'] = 'a' + file90.state90.str.cat(others=[file90.cty90,file90.tract90,file90.blk90]).astype(str)
    file90['blk2000'] = 'b' + file90.state90.str.cat(others=[file90.cty90,file90.tract00,file90.blk00]).astype(str)
    
    assert (file90.blk1990.str.len()==16).all() == True
    assert (file90.blk2000.str.len()==16).all() == True
    
    file90 = file90.loc[:,['blk1990','blk2000']]
    
    
    ###### 2) inputting 2000 to 2010 relationship files
    fileinput00 = infolder + 'TAB2000_TAB2010_ST_' + dict_state_codes[state] + '_v2.txt'
    file00 = pd.read_csv(fileinput00,dtype = str)
    
    input_vars00 = ['STATE_2000','COUNTY_2000','TRACT_2000','BLK_2000','STATE_2010','COUNTY_2010','TRACT_2010','BLK_2010']
    varslen00 = {'STATE_2000':2,'COUNTY_2000':3,'TRACT_2000':6,'BLK_2000':4,'STATE_2010':2,'COUNTY_2010':3,'TRACT_2010':6,'BLK_2010':4}
    
    # adding leading zeroes to make strings of correct length
    for var in input_vars00:    
        file00[var] = file00[var].apply(lambda x: x.zfill(varslen00[var]))        
        
    file00['blk2000'] = 'b' + file00.STATE_2000.str.cat(others=[file00.COUNTY_2000,file00.TRACT_2000,file00.BLK_2000]).astype(str)
    file00['blk2010'] = 'c' + file00.STATE_2010.str.cat(others=[file00.COUNTY_2010,file00.TRACT_2010,file00.BLK_2010]).astype(str)
        
    assert (file00.blk2000.str.len()==16).all() == True
    assert (file00.blk2010.str.len()==16).all() == True
    
    file00=file00.loc[:,['blk2000','blk2010']]


    ##### 3) checking t9t2 and t0t1 files and appending them together
    both = pd.merge(file90, file00 , indicator = True, copy = True)
    assert (both._merge == 'both').all() == True
    del both 
    
    file90.columns = ['blk1','blk2']
    file00.columns = ['blk1','blk2']
    fileall = file00.append(file90)
    
    t2 = datetime.datetime.now()
    delta_t0 = datetime.datetime.now() - t1
    print state_name + "File preparation time."
    print "Time elapsed:"
    print delta_t0
    

    ###### 4) determining connected sets    
    connect1 = connected_sets(fileall)
    connect2 = connected_sets(file00)
    connect3 = connected_sets(file90)
        
    del fileall, file00, file90
        
    t3 = datetime.datetime.now()    
    delta_t1 = datetime.datetime.now() - t2
    print state_name + "Connected Sets completed."
    print "Time elapsed:"
    print delta_t1
    

    ###### 5) outputting files     
    fileout = outfolder + 'consistent_blk_st' + dict_state_codes[state] + '.csv'
       
    table1 = make_table(connect1,'groupid1')
    table2 = make_table(connect2,'groupid2')
    table3 = make_table(connect3,'groupid3')
    
    header1 = table1.pop(0)
    header2 = table2.pop(0)
    header3 = table3.pop(0)
    
    out1 = pd.DataFrame(table1,columns = header1)
    out2 = pd.DataFrame(table2,columns = header2)
    out3 = pd.DataFrame(table3,columns = header3)
    
    both = pd.merge(out1, out2 , indicator = True, copy = True, how = 'outer')
    both.columns = ['blkid','groupid1','groupid2','_merge12']

    both = pd.merge(both, out3 , indicator = True, copy = True, how = 'outer')
    both.columns = ['blkid','groupid1','groupid2','_merge12','groupid3','_merge23']
        
    del table1, table2, table3, header1, header2, header3, out1, out2, out3
    
    t4 = datetime.datetime.now() 
    delta_t2 = datetime.datetime.now() - t3
    print state_name + "Table made."
    print "Time elapsed:"
    print delta_t2
    
    both.to_csv(fileout,columns=['blkid','groupid1','groupid2','groupid3'],index = False)

    del both
    
    delta_t3 = datetime.datetime.now() - t4
    print state_name + "File outputted"
    print "Time elapsed:"
    print delta_t3    

    delta_t4 = datetime.datetime.now() - t1
    print state_name + "ALL DONE!!!!!!!!!!!!!!!!"
    print "Total Time elapsed:"
    print delta_t4
    
    
delta_t5 = datetime.datetime.now() - t0
print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
print "ALL STATES DONE!!!!!!!!!!!!!!!!"
print "Total Time elapsed:"
print delta_t5
    
    


