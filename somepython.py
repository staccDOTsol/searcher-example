import json 
with open ('whirlpools.json', 'r') as f:
    wps = json.loads(f.read()) 
wpKeys = [] 
for wp in wps['whirlpools']:
    wpKeys.append(wp['address'])
with open ('wpKeys.json', 'w') as f:
    f.write(json.dumps(wpKeys))
    
with open ('mainnet.json', 'r') as f:
    mainnet = json.loads(f.read())
raydiumKeys = []
for pool in mainnet['official']:
    raydiumKeys.append(pool['id'])
for pool in mainnet['unOfficial']:
    raydiumKeys.append(pool['id'])
with open ('raydiumKeys.json', 'w') as f:
    f.write(json.dumps(raydiumKeys))
with open ('raydium_clmm.json', 'r') as f:
    raydium_clmm = json.loads(f.read())
raydium_clmmKeys = []
for pool in raydium_clmm['data']:
    raydium_clmmKeys.append(pool['id'])
with open ('raydium_clmmKeys.json', 'w') as f:
    f.write(json.dumps(raydium_clmmKeys))