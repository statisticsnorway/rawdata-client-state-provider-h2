/*
  Find first position returns NULL unless the first record in expected_positions is a completed position
 */
SELECT ep.namespace, ep.opaque_id
FROM expected_positions ep
WHERE ep.namespace = ?
  -- returns first completed position
  AND ep.ID = (SELECT ep2.id
               FROM expected_positions ep2
                      LEFT JOIN completed_positions cp2
                                ON ep2.namespace = cp2.namespace AND ep2.opaque_id = cp2.opaque_id AND ep2.namespace = ?
               WHERE cp2.namespace IS NOT NULL
                 AND cp2.namespace = ?
                 AND cp2.opaque_id IS NOT NULL
                 -- group by latest batch in order to support overlapping expected_positions
                 AND ep2.batch_ts IN (SELECT MAX(ep3.batch_ts)
                                      FROM expected_positions ep3
                                      WHERE ep3.namespace = ?
                                      GROUP BY ep3.batch_ts
                                      ORDER BY ep3.batch_ts)
               ORDER BY ep2.id
               LIMIT 1)
  -- compare with first expected position
  AND ep.iD = (SELECT MIN(ep2.ID)
               FROM EXPECTED_POSITIONS ep2
               WHERE ep2.namespace = ?
               GROUP BY ep2.ID
               ORDER BY ep2.ID
               LIMIT 1)

