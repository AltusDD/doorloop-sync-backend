
-- ✅ Enforce Foreign Keys (Safe: Raw Tables Only)

ALTER TABLE public.doorloop_raw_units
ADD CONSTRAINT fk_property_id FOREIGN KEY (propertyId)
REFERENCES public.doorloop_raw_properties(id);

-- Add more constraints as needed, skipping views entirely.
