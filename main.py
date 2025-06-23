from fastapi import FastAPI
from doorloop import sync_properties, sync_units, sync_leases, sync_tenants, sync_owners

app = FastAPI()

@app.get("/api/syncProperties")
def run_sync_properties():
    return sync_properties.sync()

@app.get("/api/syncUnits")
def run_sync_units():
    return sync_units.sync()

@app.get("/api/syncLeases")
def run_sync_leases():
    return sync_leases.sync()

@app.get("/api/syncTenants")
def run_sync_tenants():
    return sync_tenants.sync()

@app.get("/api/syncOwners")
def run_sync_owners():
    return sync_owners.sync()
