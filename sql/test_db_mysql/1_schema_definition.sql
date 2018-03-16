-- -----------------------------------------------------
-- Schema ml_airflow
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `ml_airflow`;
CREATE SCHEMA IF NOT EXISTS ml_airflow
  DEFAULT CHARACTER SET utf8;
USE ml_airflow;

-- -----------------------------------------------------
-- Table ml_dag
-- -----------------------------------------------------
DROP TABLE IF EXISTS ml_dag;

CREATE TABLE IF NOT EXISTS ml_dag (
  id               INT          NOT NULL    AUTO_INCREMENT,
  datetime_created DATETIME     NULL        DEFAULT CURRENT_TIMESTAMP,
  parameter_1      VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
)
  ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table ml_training_dag
-- -----------------------------------------------------
DROP TABLE IF EXISTS ml_training_dag;

CREATE TABLE IF NOT EXISTS ml_training_dag (
  id               INT          NOT NULL    AUTO_INCREMENT,
  ml_dag_id        INT          NOT NULL UNIQUE,
  parameter_2      VARCHAR(255) NOT NULL,
  datetime_created DATETIME     NULL        DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  CONSTRAINT fk_ml_training_dag_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
  ENGINE = InnoDB;

CREATE INDEX fk_ml_training_dag_ml_dag_id_idx
  ON ml_training_dag (ml_dag_id ASC);

-- -----------------------------------------------------
-- Table ml_testing_dag
-- -----------------------------------------------------
DROP TABLE IF EXISTS ml_testing_dag;

CREATE TABLE IF NOT EXISTS ml_testing_dag (
  id               INT          NOT NULL    AUTO_INCREMENT,
  ml_dag_id        INT          NOT NULL UNIQUE,
  parameter_3      VARCHAR(255) NOT NULL,
  datetime_created DATETIME     NULL        DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  CONSTRAINT fk_ml_testing_dag_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
  ENGINE = InnoDB;

CREATE INDEX fk_ml_testing_dag_ml_dag_id_idx
  ON ml_testing_dag (ml_dag_id ASC);

-- -----------------------------------------------------
-- Table common_task_1
-- -----------------------------------------------------
DROP TABLE IF EXISTS common_task_1;

CREATE TABLE IF NOT EXISTS common_task_1 (
  id                INT      NOT NULL    AUTO_INCREMENT,
  ml_dag_id         INT      NOT NULL UNIQUE,
  datetime_started  DATETIME NULL,
  datetime_finished DATETIME NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_common_task_1_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
  ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table common_task_2
-- -----------------------------------------------------
DROP TABLE IF EXISTS common_task_2;

CREATE TABLE IF NOT EXISTS common_task_2 (
  id                INT      NOT NULL    AUTO_INCREMENT,
  ml_dag_id         INT      NOT NULL UNIQUE,
  datetime_started  DATETIME NULL,
  datetime_finished DATETIME NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_common_task_2_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
  ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table training_task_1
-- -----------------------------------------------------
DROP TABLE IF EXISTS training_task_1;

CREATE TABLE IF NOT EXISTS training_task_1 (
  id                INT      NOT NULL    AUTO_INCREMENT,
  ml_dag_id         INT      NOT NULL UNIQUE,
  datetime_started  DATETIME NULL,
  datetime_finished DATETIME NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_training_task_1_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
  ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table testing_task_1
-- -----------------------------------------------------
DROP TABLE IF EXISTS testing_task_1;

CREATE TABLE IF NOT EXISTS testing_task_1 (
  id                INT      NOT NULL    AUTO_INCREMENT,
  ml_dag_id         INT      NOT NULL UNIQUE,
  datetime_started  DATETIME NULL,
  datetime_finished DATETIME NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_testing_task_1_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
  ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table testing_task_2
-- -----------------------------------------------------
DROP TABLE IF EXISTS testing_task_2;

CREATE TABLE IF NOT EXISTS testing_task_2 (
  id                INT      NOT NULL    AUTO_INCREMENT,
  ml_dag_id         INT      NOT NULL UNIQUE,
  datetime_started  DATETIME NULL,
  datetime_finished DATETIME NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_testing_task_2_ml_dag_id FOREIGN KEY (ml_dag_id) REFERENCES ml_dag (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
)
  ENGINE = InnoDB;