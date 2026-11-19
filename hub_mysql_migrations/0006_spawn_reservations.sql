-- One reservation per job, request and task role.
-- applied-if: SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='spawn_reservations'
CREATE TABLE spawn_reservations (
 job VARCHAR(255) NOT NULL,
 request_id VARCHAR(255) NOT NULL,
 task_role VARCHAR(255) NULL,
 PRIMARY KEY (job),
 UNIQUE KEY spawn_reservations_request_id (request_id),
 UNIQUE KEY spawn_reservations_task_role (task_role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
