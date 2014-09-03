CREATE TABLE oap_ids (
  oap_id       TEXT PRIMARY KEY NOT NULL,
  campus_id    TEXT NOT NULL
);

CREATE UNIQUE INDEX campus_id ON oap_ids(campus_id);
