import re
import uuid

NAMESPACE_UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")

def camel_to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def mongodb_id_to_uuid(mongo_id: str) -> str:
    return str(uuid.uuid5(NAMESPACE_UUID, mongo_id))
