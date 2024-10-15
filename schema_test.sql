DROP TABLE IF EXISTS job_test;

CREATE TABLE job_test
(
  job_id bigint NOT NULL,
  name   text   NOT NULL,
  value  text   NOT NULL,

  CONSTRAINT job_test_pkey PRIMARY KEY (job_id)
);
