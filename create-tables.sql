BEGIN;

  CREATE TABLE IF NOT EXISTS public.projects (
    "name" varchar(100) NOT NULL DEFAULT ''::character varying,
    version_id int4 NOT NULL DEFAULT 0,
    last_accessed timestamptz NULL,
    CONSTRAINT projects_pkey PRIMARY KEY (name)
  );
  CREATE INDEX IF NOT EXISTS projects_index_last_accessed
    ON public.projects
    USING btree
    (last_accessed);

  CREATE TABLE IF NOT EXISTS public.url_index_store (
    project_name varchar(100) NOT NULL DEFAULT ''::character varying,
    url varchar(2000) NOT NULL,
    "path" varchar(2000) NOT NULL,
    CONSTRAINT url_index_store_pkey PRIMARY KEY (project_name, url)
  );
  CREATE UNIQUE INDEX IF NOT EXISTS project_path_index
    ON public.url_index_store
    USING btree
    (project_name, path);
COMMIT;
