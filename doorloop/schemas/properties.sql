-- DROP TABLE IF EXISTS properties;
CREATE TABLE IF NOT EXISTS properties (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  doorLoopId TEXT,
  name TEXT,
  addressStreet1 TEXT,
  addressStreet2 TEXT,
  addressCity TEXT,
  addressState TEXT,
  zip TEXT,
  propertyType TEXT,
  class TEXT,
  status TEXT,
  totalSqFt INTEGER,
  unitCount INTEGER,
  occupiedUnits INTEGER,
  occupancyRate NUMERIC,
  yearBuilt TEXT,
  ownerName TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);
