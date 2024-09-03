-- Tags: no-parallel
-- Do not run this test in parallel because `all` workload might affect other queries execution process
CREATE OR REPLACE WORKLOAD all;
SELECT name, parent, create_query FROM system.workloads;
CREATE WORKLOAD IF NOT EXISTS production IN all;
CREATE WORKLOAD development IN all;
SELECT name, parent, create_query FROM system.workloads;
DROP WORKLOAD IF EXISTS production;
DROP WORKLOAD development;
SELECT name, parent, create_query FROM system.workloads;
DROP WORKLOAD all;
