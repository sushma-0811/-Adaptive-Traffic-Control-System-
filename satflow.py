import pandas as pd
import statistics
import time
csv = pd.read_csv("J013.csv",header=None)
timestamp = []
h_min= 0.5
scn = []
k = 0 #start
l = 86400 #end
interval = 20
for i,j in zip(csv[1],csv[5]):

    if type(i)==float :
        continue
    else:
        x = (((i.split("-")[2]).split(" ")[1]).split(":"))
     
        t = int(x[0])*3600+int(x[1])*60+float(x[2])
  
    if k<t<l:
        timestamp.append(t)
        scn.append(j.split('_')[1])
        
df = pd.DataFrame(
    {'SCN': scn,
     'timestamp': timestamp
    })
scn_final = []
for i in scn:
    if i not in scn_final:
        scn_final.append(i)
scn_final.sort()
sat_flow = []

l = len(df)

t1 = time.time()
for i in range(0, l):
    a = df.at[i,'timestamp']
    # b=df.at[i,'begin'].split(",")[0]
    df.at[i,'interval'] = interval * (int(a) // interval)
#    .split(",")[-1]
t2 = time.time()
# print(df)
# print(t2-t1)
df = df.groupby('SCN')
flow = []
for scn in scn_final:
    df_scn = df.get_group(scn)
    count = df_scn['interval'].value_counts()
    flow.append(int(count.iloc[0]/interval*3600))

sat_flow_table = pd.DataFrame(
    {
        'Link No.' : scn_final,
        'Saturation Flow' : flow
    })
sat_flow_table.to_csv('satflow.txt',index=False)



