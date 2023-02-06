create external table if not exists cards_ingest.ev_vehicle_info
(
    vin varchar(10),
    county varchar(30),
    city varchar(30),
    state varchar(2),
    postal_code varchar(5),
    model_year varchar(4),
    model string,
    electric_vehicle_type string,
    cafv_eligibility string,
    electric_range int,
    base_msrp decimal(12,2),
    legislative_district string,
    dol_vehicle_id string,
    vehicle_location string,
    electric_utility string,
    census_tract_msrp_2020 string
)
PARTITIONED BY (make string)
stored as parquet
location "s3://quintrix-spearscjs/data/cards_ingest/ev_vehicle_info/"
tblproperties ("parquet.compression"="SNAPPY", "skip.header.line.count"="1");
