DROP TABLE IF EXISTS que_jobs;

CREATE TABLE que_jobs
(
  priority    smallint    NOT NULL DEFAULT 100,
  run_at      timestamptz NOT NULL DEFAULT now(),
  job_id      bigserial   NOT NULL,
  job_class   text        NOT NULL,
  args        json        NOT NULL DEFAULT '[]'::json,
  error_count integer     NOT NULL DEFAULT 0,
  last_error  text,
  queue       text        NOT NULL DEFAULT '',

  CONSTRAINT que_jobs_pkey PRIMARY KEY (queue, priority, run_at, job_id)
);

COMMENT ON TABLE que_jobs IS '3';

DROP TABLE IF EXISTS job_test;

CREATE TABLE job_test
(
  job_id bigserial   NOT NULL,
  name   text        NOT NULL,
  queue  text        NOT NULL,

  CONSTRAINT job_test_pkey PRIMARY KEY (job_id)
);
