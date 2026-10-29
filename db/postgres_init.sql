-- Schema ecrit par Airbyte (destination)
CREATE SCHEMA IF NOT EXISTS raw;

-- Table des ventes validees (alimentee par l'asset Dagster validated_sales)
CREATE TABLE IF NOT EXISTS public.validated_sales (
  id       INT PRIMARY KEY,
  customer VARCHAR(100),
  product  VARCHAR(100),
  amount   NUMERIC(10,2),
  sold_at  TIMESTAMP
);

-- Historique des controles qualite
CREATE TABLE IF NOT EXISTS public.quality_results (
  id         SERIAL PRIMARY KEY,
  run_id     TEXT NOT NULL,
  check_name TEXT NOT NULL,
  passed     BOOLEAN NOT NULL,
  metadata   JSONB,
  checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
