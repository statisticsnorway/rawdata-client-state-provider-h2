DROP TABLE IF EXISTS completed_positions;
DROP TABLE IF EXISTS expected_positions;

CREATE TABLE expected_positions (
  id            bigint auto_increment,
  namespace     varchar                      NOT NULL,
  opaque_id     varchar                      NOT NULL,
  synthetic_id  int4                         NOT NULL,
  batch_ts      timestamp with time zone     NOT NULL,
  PRIMARY KEY (id, namespace, opaque_id, batch_ts)
);

CREATE TABLE completed_positions (
  namespace     varchar                      NOT NULL,
  opaque_id     varchar                      NOT NULL,
  PRIMARY KEY (namespace, opaque_id),
  FOREIGN KEY (namespace, opaque_id) REFERENCES expected_positions(namespace, opaque_id)
);
