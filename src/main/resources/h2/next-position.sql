/*
  Find next position returns NULL unless the next record in expected_positions is a completed position
 */
-- returns first completed position
SELECT ep.namespace, ep.opaque_id
FROM expected_positions ep
       LEFT JOIN completed_positions cp
                 ON ep.namespace = cp.namespace AND ep.opaque_id = cp.opaque_id AND cp.namespace = ?
WHERE ep.namespace = ?
  AND cp.namespace IS NULL
  AND cp.opaque_id IS NULL
  -- group by latest batch in order to support overlapping expected_positions
  AND ep.batch_ts IN (SELECT MAX(ep2.batch_ts)
                       FROM expected_positions ep2
                       WHERE ep2.namespace = ?
                       GROUP BY ep2.batch_ts
                       ORDER BY ep2.batch_ts)
ORDER BY ep.id
LIMIT 1
