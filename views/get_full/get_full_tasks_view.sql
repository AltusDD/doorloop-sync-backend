-- get_full_tasks_view.sql - Join with properties and units
create or replace view get_full_tasks_view as
select
  t.id,
  t.doorloop_id,
  t.title,
  t.description,
  t.status,
  t.due_date,
  t.property_id,
  p.name as property_name,
  t.unit_id,
  u.name as unit_name,
  t.created_at,
  t.updated_at
from doorloop_normalized_tasks t
left join doorloop_normalized_properties p on t.property_id = p.doorloop_id
left join doorloop_normalized_units u on t.unit_id = u.doorloop_id;
