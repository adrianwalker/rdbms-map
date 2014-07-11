CREATE TABLE map
(
  id serial NOT NULL,
  CONSTRAINT map_pkey PRIMARY KEY (id)
);

CREATE TABLE entry
(
  id serial NOT NULL,
  map_id integer NOT NULL,
  key_type character(1) NOT NULL,
  value_type character(1) NOT NULL,
  CONSTRAINT entry_pkey PRIMARY KEY (id),
  CONSTRAINT entry_map_id_fkey FOREIGN KEY (map_id) REFERENCES map (id) ON DELETE CASCADE
);

CREATE INDEX entry_key_type_idx ON entry (key_type);
CREATE INDEX entry_value_type_idx ON entry (value_type);

CREATE TABLE object_integer
(
  id serial NOT NULL,
  entry_id integer NOT NULL,
  map_id integer NOT NULL,
  type character(1) NOT NULL,
  value integer NOT NULL,
  CONSTRAINT object_integer_pkey PRIMARY KEY (id),
  CONSTRAINT object_integer_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry (id) ON DELETE CASCADE,
  CONSTRAINT object_integer_map_id_fkey FOREIGN KEY (map_id) REFERENCES map (id) ON DELETE CASCADE
);

CREATE INDEX object_integer_type_idx ON object_integer (type);
CREATE INDEX object_integer_value_idx ON object_integer (value);


CREATE TABLE object_boolean
(
  id serial NOT NULL,
  entry_id integer NOT NULL,
  map_id integer NOT NULL,
  type character(1) NOT NULL,
  value boolean NOT NULL,
  CONSTRAINT object_boolean_pkey PRIMARY KEY (id),
  CONSTRAINT object_boolean_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry (id) ON DELETE CASCADE,
  CONSTRAINT object_boolean_map_id_fkey FOREIGN KEY (map_id) REFERENCES map (id) ON DELETE CASCADE
);

CREATE INDEX object_boolean_type_idx ON object_boolean (type);
CREATE INDEX object_boolean_value_idx ON object_boolean (value);

CREATE TABLE object_numeric
(
  id serial NOT NULL,
  entry_id integer NOT NULL,
  map_id integer NOT NULL,
  type character(1) NOT NULL,
  value numeric NOT NULL,
  CONSTRAINT object_numeric_pkey PRIMARY KEY (id),
  CONSTRAINT object_numeric_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry (id) ON DELETE CASCADE,
  CONSTRAINT object_numeric_map_id_fkey FOREIGN KEY (map_id) REFERENCES map (id) ON DELETE CASCADE
);

CREATE INDEX object_numeric_type_idx ON object_numeric (type);
CREATE INDEX object_numeric_value_idx ON object_numeric (value);

CREATE TABLE object_text
(
  id serial NOT NULL,
  entry_id integer NOT NULL,
  map_id integer NOT NULL,
  type character(1) NOT NULL,
  value text NOT NULL,
  CONSTRAINT object_text_pkey PRIMARY KEY (id),
  CONSTRAINT object_text_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry (id) ON DELETE CASCADE,
  CONSTRAINT object_text_map_id_fkey FOREIGN KEY (map_id) REFERENCES map (id) ON DELETE CASCADE
);

CREATE INDEX object_text_type_idx ON object_text (type);
CREATE INDEX object_text_value_idx ON object_text (value);

CREATE TABLE object_null
(
  id serial NOT NULL,
  entry_id integer NOT NULL,
  map_id integer NOT NULL,
  type character(1) NOT NULL,
  CONSTRAINT object_null_pkey PRIMARY KEY (id),
  CONSTRAINT object_null_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry (id) ON DELETE CASCADE,
  CONSTRAINT object_null_map_id_fkey FOREIGN KEY (map_id) REFERENCES map (id) ON DELETE CASCADE
);

CREATE INDEX object_null_type_idx ON object_null (type);

CREATE TABLE object_map
(
  id serial NOT NULL,
  entry_id integer NOT NULL,
  map_id integer NOT NULL,
  type character(1) NOT NULL,
  value integer NOT NULL,
  CONSTRAINT object_map_pkey PRIMARY KEY (id),
  CONSTRAINT object_map_entry_id_fkey FOREIGN KEY (entry_id) REFERENCES entry (id) ON DELETE CASCADE,
  CONSTRAINT object_map_map_id_fkey FOREIGN KEY (map_id) REFERENCES map (id) ON DELETE CASCADE,
  CONSTRAINT object_map_value_fkey FOREIGN KEY (value) REFERENCES map (id) ON DELETE CASCADE
);

CREATE INDEX object_map_type_idx ON object_map (type);
