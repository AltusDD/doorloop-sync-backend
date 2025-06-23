import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI
from doorloop import (
    sync_properties, sync_units, sync_leases, sync_tenants, sync_owners,
    sync_payments, sync_charges, sync_credits, sync_vendors,
    sync_tasks, sync_notes, sync_files, sync_accounts, sync_users
)

app = FastAPI()

@app.get("/api/syncProperties")
def sync_props():
    return sync_properties.sync()

@app.get("/api/syncUnits")
def sync_units_():
    return sync_units.sync()

@app.get("/api/syncLeases")
def sync_leases_():
    return sync_leases.sync()

@app.get("/api/syncTenants")
def sync_tenants_():
    return sync_tenants.sync()

@app.get("/api/syncOwners")
def sync_owners_():
    return sync_owners.sync()

@app.get("/api/syncPayments")
def sync_payments_():
    return sync_payments.sync()

@app.get("/api/syncCharges")
def sync_charges_():
    return sync_charges.sync()

@app.get("/api/syncCredits")
def sync_credits_():
    return sync_credits.sync()

@app.get("/api/syncVendors")
def sync_vendors_():
    return sync_vendors.sync()

@app.get("/api/syncTasks")
def sync_tasks_():
    return sync_tasks.sync()

@app.get("/api/syncNotes")
def sync_notes_():
    return sync_notes.sync()

@app.get("/api/syncFiles")
def sync_files_():
    return sync_files.sync()

@app.get("/api/syncAccounts")
def sync_accounts_():
    return sync_accounts.sync()

@app.get("/api/syncUsers")
def sync_users_():
    return sync_users.sync()
