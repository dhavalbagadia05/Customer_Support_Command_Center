-- Reference: country dimension (small, static) with region_group, lat/lon, subscribers, language
CREATE OR REFRESH MATERIALIZED VIEW bronze_regions (
  CONSTRAINT valid_country EXPECT (country_code IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_region_group EXPECT (region_group IN ('NA', 'LATAM', 'EMEA', 'APAC'))
)
COMMENT 'Country dimension.'
AS SELECT * FROM classic_stable_l0m1dn_catalog.support_command_center.regions;
