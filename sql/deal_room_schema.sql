-- deal_room_schema.sql
-- SQL definitions for Deal Room tables

create table if not exists public.deals (
    id bigint generated always as identity primary key,
    name text not null,
    status text not null default 'draft',
    created_at timestamptz default timezone('utc'::text, now()),
    updated_at timestamptz default timezone('utc'::text, now())
);

create table if not exists public.investors (
    id bigint generated always as identity primary key,
    full_name text not null,
    email text unique,
    phone text,
    created_at timestamptz default timezone('utc'::text, now())
);

create table if not exists public.deal_investments (
    id bigint generated always as identity primary key,
    deal_id bigint not null references public.deals(id) on delete cascade,
    investor_id bigint not null references public.investors(id) on delete cascade,
    amount numeric,
    created_at timestamptz default timezone('utc'::text, now()),
    unique (deal_id, investor_id)
);
