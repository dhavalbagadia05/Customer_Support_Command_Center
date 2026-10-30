-- Reference: anime simulcast release calendar with expected traffic uplift
CREATE OR REFRESH MATERIALIZED VIEW bronze_release_schedule (
  CONSTRAINT valid_release EXPECT (release_id IS NOT NULL AND release_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_uplift EXPECT (expected_uplift >= 0)
)
COMMENT 'Simulcast release schedule.'
AS SELECT * FROM classic_stable_l0m1dn_catalog.support_command_center.release_schedule;
