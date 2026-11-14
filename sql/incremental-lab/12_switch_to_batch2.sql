
-- Switch from Batch 1 to Batch 2
UPDATE dbt_lab.batch_control
SET current_batch = 2
WHERE control_id = 1
  AND current_batch = 1;

-- Confirm batch
SELECT *
FROM dbt_lab.batch_control;
