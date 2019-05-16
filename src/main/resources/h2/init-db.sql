DROP TABLE IF EXISTS next_position;
DROP TABLE IF EXISTS completed_positions;

CREATE TABLE next_position (
  namespace     varchar                      NOT NULL,
  opaque_id     varchar                      NOT NULL,
  PRIMARY KEY (namespace, opaque_id),
);

CREATE TABLE completed_positions (
  id            bigint auto_increment,
  namespace     varchar                      NOT NULL,
  opaque_id     varchar                      NOT NULL,
  PRIMARY KEY (id, namespace, opaque_id),
);
