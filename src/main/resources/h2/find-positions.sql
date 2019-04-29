/*
  Find from/to positions and returns NULL unless the next record in expected_positions is a completed position
 */
SELECT ep.namespace, ep.opaque_id
FROM expected_positions ep
WHERE ep.namespace = ?
  -- returns first completed position. If all positions are completed it will return null.
  AND ep.id IN (
              SELECT COALESCE(s1.id, s2.id) as id
              FROM (SELECT ep2.id as id FROM expected_positions ep2 WHERE ep2.id < (SELECT ep3.id
                             FROM expected_positions ep3
                                    LEFT JOIN completed_positions cp3
                                              ON ep3.namespace = cp3.namespace AND ep3.opaque_id = cp3.opaque_id AND cp3.namespace = ?
                             WHERE ep3.namespace = ?
                               AND cp3.namespace IS NULL
                               AND cp3.opaque_id IS NULL
                               -- group by latest batch in order to support overlapping expected_positions
                               AND ep3.batch_ts IN (SELECT MAX(ep3.batch_ts)
                                                    FROM expected_positions ep3
                                                    WHERE ep3.namespace = ?
                                                    GROUP BY ep3.batch_ts
                                                    ORDER BY ep3.batch_ts)
                             ORDER BY ep3.id LIMIT 1)
                      AND ep2.id >= (SELECT ep4.id FROM expected_positions ep4 WHERE ep4.namespace = ? AND ep4.opaque_id = ?)
                      AND ep2.id <= (SELECT ep4.id FROM expected_positions ep4 WHERE ep4.namespace = ? AND ep4.opaque_id = ?)
                             ) as s1
              RIGHT JOIN
              (SELECT ep2.id as id
                             FROM expected_positions ep2
                                    LEFT JOIN completed_positions cp2
                                              ON ep2.namespace = cp2.namespace AND ep2.opaque_id = cp2.opaque_id AND cp2.namespace = ?
                             WHERE ep2.namespace = ?
                               AND ep2.id >= (SELECT ep4.id FROM expected_positions ep4 WHERE ep4.namespace = ? AND ep4.opaque_id = ?)
                               AND ep2.id <= (SELECT ep4.id FROM expected_positions ep4 WHERE ep4.namespace = ? AND ep4.opaque_id = ?)
                               AND cp2.namespace IS NOT NULL
                               AND cp2.opaque_id IS NOT NULL
                               -- group by latest batch in order to support overlapping expected_positions
                               AND ep2.batch_ts IN (SELECT MAX(ep3.batch_ts)
                                                    FROM expected_positions ep3
                                                    WHERE ep3.namespace = ?
                                                    GROUP BY ep3.batch_ts
                                                    ORDER BY ep3.batch_ts)
                             ORDER BY ep2.id
                             ) as s2
              GROUP BY s1.id, s2.id
  )
  -- compare with first expected position
  AND ep.id >= (SELECT MIN(ep2.id)
               FROM expected_positions ep2
               WHERE ep2.namespace = ?
               GROUP BY ep2.ID
               ORDER BY ep2.ID
               LIMIT 1)


