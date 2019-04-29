/*
    Find last position
 */
SELECT DISTINCT COALESCE(s1.id, s2.id) as id
                -- last position when there are remaining positions to be completed
FROM (SELECT ep2.opaque_id as id
      FROM expected_positions ep2
      WHERE ep2.id < (SELECT ep3.id
                      FROM expected_positions ep3
                             LEFT JOIN completed_positions cp3
                                       ON ep3.namespace = cp3.namespace AND ep3.opaque_id = cp3.opaque_id AND
                                          cp3.namespace = ?
                      WHERE ep3.namespace = ?
                        AND cp3.namespace IS NULL
                        AND cp3.opaque_id IS NULL
                        -- group by latest batch in order to support overlapping expected_positions
                        AND ep3.batch_ts IN (SELECT MAX(ep3.batch_ts)
                                             FROM expected_positions ep3
                                             WHERE ep3.namespace = ?
                                             GROUP BY ep3.batch_ts
                                             ORDER BY ep3.batch_ts)
                      ORDER BY ep3.id ASC
                      LIMIT 1)
      ORDER BY ep2.id DESC
      LIMIT 1) as s1
       RIGHT JOIN
     -- last when all positions are completed
       (SELECT ep2.opaque_id as id
        FROM expected_positions ep2
               LEFT JOIN completed_positions cp2
                         ON ep2.namespace = cp2.namespace AND ep2.opaque_id = cp2.opaque_id AND cp2.namespace = ?
        WHERE ep2.namespace = ?
          AND cp2.namespace IS NOT NULL
          AND cp2.opaque_id IS NOT NULL
          -- group by latest batch in order to support overlapping expected_positions
          AND ep2.batch_ts IN (SELECT MAX(ep3.batch_ts)
                               FROM expected_positions ep3
                               WHERE ep3.namespace = ?
                               GROUP BY ep3.batch_ts
                               ORDER BY ep3.batch_ts)
        ORDER BY ep2.id DESC
        LIMIT 1) as s2
GROUP BY s1.id, s2.id
