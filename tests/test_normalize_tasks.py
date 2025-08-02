
import pytest
from doorloop_sync.tasks.normalize import normalize_lease_payments, normalize_vendors, normalize_tasks

class MockSupabaseIngestClient:
    def __init__(self):
        self.data = []

    def fetch(self, table):
        if "lease_payments" in table:
            return [{
                "id": "p1", "leaseId": "l1", "amount": "1000", "date": "2023-07-01",
                "method": "cash", "createdAt": "2023-07-01T00:00:00Z", "updatedAt": "2023-07-02T00:00:00Z"
            }]
        elif "vendors" in table:
            return [{
                "id": "v1", "name": "ACME Plumbing", "category": "Plumbing",
                "phone": "123-456-7890", "email": "acme@example.com", "status": "active",
                "createdAt": "2023-01-01T00:00:00Z", "updatedAt": "2023-01-02T00:00:00Z"
            }]
        elif "tasks" in table:
            return [{
                "id": "t1", "title": "Fix Sink", "status": "open", "priority": "high",
                "propertyId": "p123", "dueDate": "2023-08-01",
                "createdAt": "2023-07-01T00:00:00Z", "updatedAt": "2023-07-02T00:00:00Z"
            }]
        return []

    def upsert(self, table, data):
        self.data.append((table, data))

@pytest.fixture
def client():
    return MockSupabaseIngestClient()

def test_normalize_lease_payments(client):
    normalize_lease_payments.run(client)
    assert client.data[0][0] == "doorloop_normalized_lease_payments"
    assert len(client.data[0][1]) == 1
    assert "amount" in client.data[0][1][0]

def test_normalize_vendors(client):
    normalize_vendors.run(client)
    assert client.data[0][0] == "doorloop_normalized_vendors"
    assert "name" in client.data[0][1][0]

def test_normalize_tasks(client):
    normalize_tasks.run(client)
    assert client.data[0][0] == "doorloop_normalized_tasks"
    assert "title" in client.data[0][1][0]
