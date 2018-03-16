-- -----------------------------------------------------
-- Inserts
-- -----------------------------------------------------
INSERT INTO ml_dag (id, parameter_1) VALUES (1, 'test_parameter_1');

INSERT INTO ml_training_dag (id, ml_dag_id, parameter_2) VALUES (1, 1, 'test_parameter_2');
INSERT INTO ml_testing_dag (id, ml_dag_id, parameter_3) VALUES (1, 1, 'test_parameter_3');

INSERT INTO common_task_1 (ml_dag_id) VALUES (1);
INSERT INTO common_task_2 (ml_dag_id) VALUES (1);
INSERT INTO testing_task_1 (ml_dag_id) VALUES (1);
INSERT INTO testing_task_2 (ml_dag_id) VALUES (1);
INSERT INTO training_task_1 (ml_dag_id) VALUES (1);