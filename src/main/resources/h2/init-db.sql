DROP TABLE IF EXISTS completed_positions;

CREATE TABLE completed_positions (
  id            bigint auto_increment,
  namespace     varchar                      NOT NULL,
  opaque_id     varchar                      NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (namespace, opaque_id)
);
