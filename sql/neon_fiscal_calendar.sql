-- Neon fiscal calendar dimension (draft; not executed). Mirrors Power BI Calendar (Beachwood-PowerBI) / Dataverse crf63_bwfiscalcalendars.
-- Joins:
-- * Date-grain facts join crf63_date: crf63_calendardate on crf63_oarsbidatas, crf63_saleschanneldailies, crf63_offerses, crf63_inventories, and crf63_bw_clockinouts; crf63_workdate on crf63_labortimes.
-- * crf63_bwsalesforecasts.crf63_forecastedfiscalweek joins fyw.
CREATE TABLE IF NOT EXISTS crf63_bwfiscalcalendars (
    crf63_date                      date PRIMARY KEY,
    crf63_dayofyear                 integer NOT NULL,
    crf63_calendardayofyear         integer NOT NULL,
    crf63_fiscalyearnumber          integer NOT NULL,
    crf63_fiscalyearlabel           text    NOT NULL,
    crf63_fiscalquarternumber       integer NOT NULL,
    crf63_fiscalquarterlabel        text    NOT NULL,
    crf63_fiscalperiodnumber        integer NOT NULL,
    crf63_fiscalperiodlabel         text    NOT NULL,
    crf63_fiscalweeknumber          integer NOT NULL,
    crf63_fiscalweeklabel           text    NOT NULL,
    crf63__445fiscalperiodnumber    integer NOT NULL,
    crf63__445fiscalperiodlabel     text    NOT NULL,
    crf63__445fiscalquarternumber   integer NOT NULL,
    crf63__445fiscalquarterlabel    text    NOT NULL,
    fyw  text GENERATED ALWAYS AS (crf63_fiscalyearlabel || crf63_fiscalweeklabel) STORED,
    fyp  text GENERATED ALWAYS AS (crf63_fiscalyearlabel || crf63_fiscalperiodlabel) STORED,
    day_of_week   integer GENERATED ALWAYS AS (EXTRACT(ISODOW FROM crf63_date)::integer) STORED,
    week_end_date date    GENERATED ALWAYS AS (crf63_date + (7 - EXTRACT(ISODOW FROM crf63_date)::integer)) STORED
);
CREATE INDEX IF NOT EXISTS ix_fiscalcal_fy_fw  ON crf63_bwfiscalcalendars (crf63_fiscalyearnumber, crf63_fiscalweeknumber);
CREATE INDEX IF NOT EXISTS ix_fiscalcal_fy_fp  ON crf63_bwfiscalcalendars (crf63_fiscalyearnumber, crf63_fiscalperiodnumber);
CREATE INDEX IF NOT EXISTS ix_fiscalcal_fy_fq  ON crf63_bwfiscalcalendars (crf63_fiscalyearnumber, crf63_fiscalquarternumber);
CREATE INDEX IF NOT EXISTS ix_fiscalcal_fy_doy ON crf63_bwfiscalcalendars (crf63_fiscalyearnumber, crf63_dayofyear);
CREATE INDEX IF NOT EXISTS ix_fiscalcal_fyw    ON crf63_bwfiscalcalendars (fyw);
CREATE INDEX IF NOT EXISTS ix_fiscalcal_wkend  ON crf63_bwfiscalcalendars (week_end_date);
