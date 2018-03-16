-- -----------------------------------------------------
-- Schema ml_airflow
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Table ml_dag
-- -----------------------------------------------------
DROP TABLE IF EXISTS ml_dag;

CREATE TABLE ml_dag (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  datetime_created DATETIME            DEFAULT CURRENT_TIMESTAMP,
  parameter_1      VARCHAR(255) NOT NULL
);

-- -----------------------------------------------------
-- Table ml_training_dag
-- -----------------------------------------------------
DROP TABLE IF EXISTS ml_training_dag;

CREATE TABLE ml_training_dag (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  ml_dag_id        INTEGER UNIQUE NOT NULL,
  parameter_2      VARCHAR(255)   NOT NULL,
  datetime_created DATETIME            DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX ix_ml_training_dag_ml_dag_id
  ON ml_training_dag (ml_dag_id ASC);

-- -----------------------------------------------------
-- Table ml_testing_dag
-- -----------------------------------------------------
DROP TABLE IF EXISTS ml_testing_dag;

CREATE TABLE ml_testing_dag (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  ml_dag_id        INTEGER UNIQUE NOT NULL,
  parameter_3      VARCHAR(255)   NOT NULL,
  datetime_created DATETIME            DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX ix_ml_testing_dag_ml_dag_id
  ON ml_testing_dag (ml_dag_id ASC);

-- -----------------------------------------------------
-- Table common_task_1
-- -----------------------------------------------------
DROP TABLE IF EXISTS common_task_1;

CREATE TABLE common_task_1 (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  ml_dag_id         INTEGER UNIQUE NOT NULL,
  datetime_started  DATETIME,
  datetime_finished DATETIME,
  CONSTRAINT fk_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
);

-- -----------------------------------------------------
-- Table common_task_2
-- -----------------------------------------------------
DROP TABLE IF EXISTS common_task_2;

CREATE TABLE common_task_2 (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  ml_dag_id         INTEGER UNIQUE NOT NULL,
  datetime_started  DATETIME,
  datetime_finished DATETIME,
  CONSTRAINT fk_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
);

-- -----------------------------------------------------
-- Table training_task_1
-- -----------------------------------------------------
DROP TABLE IF EXISTS training_task_1;

CREATE TABLE training_task_1 (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  ml_dag_id         INTEGER UNIQUE NOT NULL,
  datetime_started  DATETIME,
  datetime_finished DATETIME,
  CONSTRAINT fk_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
);

-- -----------------------------------------------------
-- Table testing_task_1
-- -----------------------------------------------------
DROP TABLE IF EXISTS testing_task_1;

CREATE TABLE testing_task_1 (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  ml_dag_id         INTEGER UNIQUE NOT NULL,
  datetime_started  DATETIME,
  datetime_finished DATETIME,
  CONSTRAINT fk_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
);

-- -----------------------------------------------------
-- Table testing_task_2
-- -----------------------------------------------------
DROP TABLE IF EXISTS testing_task_2;

CREATE TABLE testing_task_2 (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  ml_dag_id         INTEGER UNIQUE NOT NULL,
  datetime_started  DATETIME,
  datetime_finished DATETIME,
  CONSTRAINT fk_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
);