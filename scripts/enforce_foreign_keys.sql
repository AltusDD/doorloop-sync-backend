-- 🚨 Foreign Key Enforcement for Altus Empire Normalized Tables

-- 🔗 Units → Properties
ALTER TABLE normalized_units
  ADD CONSTRAINT fk_units_property
  FOREIGN KEY (property_id)
  REFERENCES normalized_properties(id)
  ON DELETE CASCADE;

-- 🔗 Leases → Units
ALTER TABLE normalized_leases
  ADD CONSTRAINT fk_leases_unit
  FOREIGN KEY (unit_id)
  REFERENCES normalized_units(id)
  ON DELETE CASCADE;

-- 🔗 Leases → Tenants
ALTER TABLE normalized_leases
  ADD CONSTRAINT fk_leases_tenant
  FOREIGN KEY (tenant_id)
  REFERENCES normalized_tenants(id)
  ON DELETE CASCADE;

-- 🔗 Leases → Owners (nullable link)
ALTER TABLE normalized_leases
  ADD CONSTRAINT fk_leases_owner
  FOREIGN KEY (owner_id)
  REFERENCES normalized_owners(id)
  ON DELETE SET NULL;

-- 🔗 Units → Owners (nullable link)
ALTER TABLE normalized_units
  ADD CONSTRAINT fk_units_owner
  FOREIGN KEY (owner_id)
  REFERENCES normalized_owners(id)
  ON DELETE SET NULL;

-- ⚡ Performance Indexes
CREATE INDEX IF NOT EXISTS idx_units_property_id ON normalized_units(property_id);
CREATE INDEX IF NOT EXISTS idx_units_owner_id ON normalized_units(owner_id);
CREATE INDEX IF NOT EXISTS idx_leases_unit_id ON normalized_leases(unit_id);
CREATE INDEX IF NOT EXISTS idx_leases_tenant_id ON normalized_leases(tenant_id);
CREATE INDEX IF NOT EXISTS idx_leases_owner_id ON normalized_leases(owner_id);
