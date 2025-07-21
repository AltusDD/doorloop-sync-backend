-- ✅ Enforce foreign keys only on actual TABLEs, not views

ALTER TABLE doorloop_normalized_units
ADD CONSTRAINT fk_property_id FOREIGN KEY (property_id) REFERENCES doorloop_normalized_properties(id);
