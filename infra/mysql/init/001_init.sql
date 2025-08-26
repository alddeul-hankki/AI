-- 3-1) DB/계정
CREATE DATABASE IF NOT EXISTS solmeal
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

-- CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%' IDENTIFIED BY '${MYSQL_PASSWORD}';
-- GRANT ALL PRIVILEGES ON ${MYSQL_DATABASE}.* TO '${MYSQL_USER}'@'%';
-- FLUSH PRIVILEGES;

USE solmeal;

-- 3-2) 스냅샷 스키마

-- run: 스냅샷 헤더
CREATE TABLE IF NOT EXISTS run (
  run_id        BIGINT PRIMARY KEY AUTO_INCREMENT,
  campus_id     BIGINT NOT NULL,
  status        ENUM('draft','active','failed') NOT NULL DEFAULT 'draft',
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  activated_at  DATETIME NULL,
  algo          VARCHAR(64) NOT NULL,
  param_json    JSON NULL,
  KEY ix_run_campus_status_created (campus_id, status, created_at DESC)
) ENGINE=InnoDB;

-- cluster_member: 사용자 ↔ 클러스터 매핑
CREATE TABLE IF NOT EXISTS cluster_member (
  id                 BIGINT PRIMARY KEY AUTO_INCREMENT,
  run_id             BIGINT NOT NULL,
  cluster_seq        INT NOT NULL,
  user_id            BIGINT NOT NULL,
  rank_in_cluster    INT NULL,
  distance_to_center DOUBLE NULL,
  CONSTRAINT fk_cm_run FOREIGN KEY (run_id) REFERENCES run(run_id) ON DELETE RESTRICT,
  CONSTRAINT uk_run_user UNIQUE (run_id, user_id),
  KEY ix_run_cluster (run_id, cluster_seq),
  KEY ix_run_cluster_rank (run_id, cluster_seq, rank_in_cluster)
) ENGINE=InnoDB;

-- campus_latest: 활성 스냅샷 포인터
CREATE TABLE IF NOT EXISTS campus_latest (
  campus_id     BIGINT PRIMARY KEY,
  active_run_id BIGINT NOT NULL,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_latest_run FOREIGN KEY (active_run_id) REFERENCES run(run_id) ON DELETE RESTRICT
) ENGINE=InnoDB;

-- 3-3) 샘플 시드 (원하면 주석 해제)
-- INSERT INTO run (campus_id, status, algo, param_json) VALUES
-- (1001, 'draft', 'baseline-v0', JSON_OBJECT('note','first'));
-- SET @rid = LAST_INSERT_ID();
-- INSERT INTO cluster_member (run_id, cluster_seq, user_id, rank_in_cluster, distance_to_center) VALUES
-- (@rid, 1, 101, 1, 0.12), (@rid, 1, 102, 2, 0.23), (@rid, 2, 201, 1, 0.05);
-- INSERT INTO campus_latest (campus_id, active_run_id) VALUES (1001, 0) ON DUPLICATE KEY UPDATE active_run_id=VALUES(active_run_id);

-- 3-4) AI용 timetable_bit
CREATE TABLE IF NOT EXISTS timetable_bit (
  id           BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id      BIGINT      NOT NULL,
  day_of_week  TINYINT     NOT NULL,
  slot1        INT UNSIGNED NOT NULL DEFAULT 0,
  slot2        INT UNSIGNED NOT NULL DEFAULT 0,
  slot3        INT UNSIGNED NOT NULL DEFAULT 0,
  slot4        INT UNSIGNED NOT NULL DEFAULT 0,
  slot5        INT UNSIGNED NOT NULL DEFAULT 0,
  slot6        INT UNSIGNED NOT NULL DEFAULT 0,
  slot7        INT UNSIGNED NOT NULL DEFAULT 0,
  slot8        INT UNSIGNED NOT NULL DEFAULT 0,
  slot9        INT UNSIGNED NOT NULL DEFAULT 0,
  is_dirty     TINYINT(1)  NOT NULL DEFAULT 1,
  created_at   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_user_dow (user_id, day_of_week),
  KEY ix_dirty (day_of_week, is_dirty),
  KEY ix_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
