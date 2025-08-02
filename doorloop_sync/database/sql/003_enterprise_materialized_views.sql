
-- Filename: 003_enterprise_materialized_views.sql
-- Empire-Grade Secure + Performant Materialized Views with Access Functions

-- Create private schema
CREATE SCHEMA IF NOT EXISTS private;

-- DROP VIEWS AND MATERIALIZED VIEWS (both public + private to ensure clean state)
DROP VIEW IF EXISTS get_full_properties_view;
DROP MATERIALIZED VIEW IF EXISTS get_full_properties_view;
DROP VIEW IF EXISTS private.get_full_properties_view;
DROP MATERIALIZED VIEW IF EXISTS private.get_full_properties_view;

DROP VIEW IF EXISTS get_full_units_view;
DROP MATERIALIZED VIEW IF EXISTS get_full_units_view;
DROP VIEW IF EXISTS private.get_full_units_view;
DROP MATERIALIZED VIEW IF EXISTS private.get_full_units_view;

DROP VIEW IF EXISTS get_full_leases_view;
DROP MATERIALIZED VIEW IF EXISTS get_full_leases_view;
DROP VIEW IF EXISTS private.get_full_leases_view;
DROP MATERIALIZED VIEW IF EXISTS private.get_full_leases_view;

DROP VIEW IF EXISTS get_full_tenants_view;
DROP MATERIALIZED VIEW IF EXISTS get_full_tenants_view;
DROP VIEW IF EXISTS private.get_full_tenants_view;
DROP MATERIALIZED VIEW IF EXISTS private.get_full_tenants_view;

DROP VIEW IF EXISTS get_full_owners_view;
DROP MATERIALIZED VIEW IF EXISTS get_full_owners_view;
DROP VIEW IF EXISTS private.get_full_owners_view;
DROP MATERIALIZED VIEW IF EXISTS private.get_full_owners_view;

-- Create MATERIALIZED VIEWS in private schema
CREATE MATERIALIZED VIEW private.get_full_properties_view AS
SELECT
    p.*,
    o.name AS owner_name,
    o.phone AS owner_phone,
    o.email AS owner_email
FROM doorloop_normalized_properties p
LEFT JOIN doorloop_normalized_owners o ON p.owner_id = o.id;

CREATE MATERIALIZED VIEW private.get_full_units_view AS
SELECT
    u.*,
    p.name AS property_name,
    p.address_street1,
    p.address_city,
    p.address_state,
    p.zip
FROM doorloop_normalized_units u
LEFT JOIN doorloop_normalized_properties p ON u.property_id = p.id;

CREATE MATERIALIZED VIEW private.get_full_leases_view AS
SELECT
    l.*,
    p.name AS property_name,
    u.unit_number,
    u.name AS unit_name
FROM doorloop_normalized_leases l
LEFT JOIN doorloop_normalized_properties p ON l.property_id = p.id
LEFT JOIN doorloop_normalized_units u ON l.unit_id = u.id;

CREATE MATERIALIZED VIEW private.get_full_tenants_view AS
SELECT
    t.*,
    l.name AS lease_name,
    l.property_id,
    l.unit_id
FROM doorloop_normalized_tenants t
LEFT JOIN doorloop_normalized_leases l ON t.lease_id = l.id;

CREATE MATERIALIZED VIEW private.get_full_owners_view AS
SELECT * FROM doorloop_normalized_owners;

-- Create SECURITY DEFINER functions in public schema
CREATE OR REPLACE FUNCTION public.get_full_properties()
RETURNS SETOF private.get_full_properties_view
LANGUAGE sql SECURITY DEFINER
SET search_path = ''
AS $$ SELECT * FROM private.get_full_properties_view $$;

CREATE OR REPLACE FUNCTION public.get_full_units()
RETURNS SETOF private.get_full_units_view
LANGUAGE sql SECURITY DEFINER
SET search_path = ''
AS $$ SELECT * FROM private.get_full_units_view $$;

CREATE OR REPLACE FUNCTION public.get_full_leases()
RETURNS SETOF private.get_full_leases_view
LANGUAGE sql SECURITY DEFINER
SET search_path = ''
AS $$ SELECT * FROM private.get_full_leases_view $$;

CREATE OR REPLACE FUNCTION public.get_full_tenants()
RETURNS SETOF private.get_full_tenants_view
LANGUAGE sql SECURITY DEFINER
SET search_path = ''
AS $$ SELECT * FROM private.get_full_tenants_view $$;

CREATE OR REPLACE FUNCTION public.get_full_owners()
RETURNS SETOF private.get_full_owners_view
LANGUAGE sql SECURITY DEFINER
SET search_path = ''
AS $$ SELECT * FROM private.get_full_owners_view $$;

-- Create invoker views in public schema that respect RLS
CREATE VIEW public.get_full_properties_view WITH (security_invoker = true) AS
SELECT
    p.*,
    o.name AS owner_name,
    o.phone AS owner_phone,
    o.email AS owner_email
FROM doorloop_normalized_properties p
LEFT JOIN doorloop_normalized_owners o ON p.owner_id = o.id;

CREATE VIEW public.get_full_units_view WITH (security_invoker = true) AS
SELECT
    u.*,
    p.name AS property_name,
    p.address_street1,
    p.address_city,
    p.address_state,
    p.zip
FROM doorloop_normalized_units u
LEFT JOIN doorloop_normalized_properties p ON u.property_id = p.id;

CREATE VIEW public.get_full_leases_view WITH (security_invoker = true) AS
SELECT
    l.*,
    p.name AS property_name,
    u.unit_number,
    u.name AS unit_name
FROM doorloop_normalized_leases l
LEFT JOIN doorloop_normalized_properties p ON l.property_id = p.id
LEFT JOIN doorloop_normalized_units u ON l.unit_id = u.id;

CREATE VIEW public.get_full_tenants_view WITH (security_invoker = true) AS
SELECT
    t.*,
    l.name AS lease_name,
    l.property_id,
    l.unit_id
FROM doorloop_normalized_tenants t
LEFT JOIN doorloop_normalized_leases l ON t.lease_id = l.id;

CREATE VIEW public.get_full_owners_view WITH (security_invoker = true) AS
SELECT * FROM doorloop_normalized_owners;
