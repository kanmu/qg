DROP TABLE IF EXISTS job_test;

CREATE TABLE job_test
(
  job_id bigserial   NOT NULL,
  name   text        NOT NULL,
  queue  text        NOT NULL,

  CONSTRAINT job_test_pkey PRIMARY KEY (job_id)
);
