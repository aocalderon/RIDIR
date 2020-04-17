#%%
from geosnap import Community 

#%%
fip = '48'
out1 =  '/tmp/TX_2000.wkt'
out2 = '/tmp/TX_2010.wkt'

#%%
data = Community.from_census(state_fips='48') 
dataset = data.gdf[ ['geometry', 'geoid', 'year'] ]  

#%%
D_2000 = dataset[(dataset.year == 2000)]
D_2000.to_csv(out1, sep='\t', index=False, header=False) 

#%%
D_2010 = dataset[(dataset.year == 2010)] 
D_2010.to_csv(out2, sep='\t', index=False, header=False)  
