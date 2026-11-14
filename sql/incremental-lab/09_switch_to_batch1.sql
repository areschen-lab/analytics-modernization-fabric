
-- Switch simulation from Batch 0 to Batch 1

UPDATE dbt_lab.batch_control
SET current_batch = 1
WHERE control_id = 1
  AND current_batch = 0;

-- Verify current batch
SELECT *
FROM dbt_lab.batch_control;
