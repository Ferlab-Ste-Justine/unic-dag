-- noinspection SqlNoDataSourceInspectionForFile

-- create infections table
CREATE TABLE IF NOT EXISTS indicateurs_sip.infections (
    currentYear VARCHAR,
    indicatorName VARCHAR,
    indicatorValueOnP1 DECIMAL,
    indicatorValueOnP2 DECIMAL,
    indicatorValueOnP3 DECIMAL,
    indicatorValueOnP4 DECIMAL,
    indicatorValueOnP5 DECIMAL,
    indicatorValueOnP6 DECIMAL,
    indicatorValueOnP7 DECIMAL,
    indicatorValueOnP8 DECIMAL,
    indicatorValueOnP9 DECIMAL,
    indicatorValueOnP10 DECIMAL,
    indicatorValueOnP11 DECIMAL,
    indicatorValueOnP12 DECIMAL,
    indicatorValueOnP13 DECIMAL,
    totalIndicatorValue DECIMAL,
    indicatorValueOnPreviousYear INT,
    denominator VARCHAR
);