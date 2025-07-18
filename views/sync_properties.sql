TRUNCATE TABLE properties RESTART IDENTITY CASCADE;
INSERT INTO properties (doorloopid, name, addressstreet1, addresscity, addressstate, zip, propertytype, class, status, totalsqft, unitcount, occupiedunits, occupancyrate, ownername, created_at, updated_at)
SELECT doorloopid, name, addressstreet1, addresscity, addressstate, zip, propertytype, class, status, totalsqft, unitcount, occupiedunits, occupancyrate, ownername, created_at, updated_at
FROM normalized_properties;