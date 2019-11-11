-- Underlying QSO table containing all modification history.
--
-- A QSO entry is uniquely defined by its qsoid. Modifications to the entry
-- should add new entries with the same qsoid and a more recent modified date,
-- leaving old versions untouched.  Deleting a QSO should add an entry with the
-- same qsoid, all fields empty except for a more recent modified date, leaving
-- existing versions untouched.
--
-- This table (`qsos_raw`) is not meant to be accessed directly. Use the view
-- `qsos` defined further below to do the actual database operations.
CREATE TABLE qsos_raw
    (qsoid INTEGER NOT NULL,
    contestname VARCHAR(10),
    timestamp TIMESTAMP,
    band VARCHAR(10),
    rxfreq FLOAT,
    txfreq FLOAT,
    countryprefix VARCHAR(8),
    operator VARCHAR(20),
    mode VARCHAR(6),
    call VARCHAR(15),
    snt VARCHAR(20),
    rcv VARCHAR(20),
    comment VARCHAR(60),
    continent VARCHAR(2),
    modified TIMESTAMP NOT NULL DEFAULT NOW(),
    modifiedby VARCHAR(40),
    PRIMARY KEY (qsoid, modified));

-- Sequence for obtaining new, unique qsoids.
CREATE SEQUENCE qsoid_generator;

-- QSO view which shows the last modified version of a QSO entry, ignores
-- deleted QSOs.
--
-- Do INSERT, DELETE and UPDATE operations on this view, as it handles proper
-- insertion of new entries with correct qsoid and modification time to
-- `qsos_raw` above. Trigger definitions that enable this are further below.
--
-- (Thus, `qsos` provide all necessary abstractions so that a user can use this
-- view for all operations instead of having to deal with `qsos_raw` directly.)
CREATE VIEW qsos AS
    SELECT f.qsoid,
        contestname,
        timestamp,
        band,
        rxfreq,
        txfreq,
        countryprefix,
        operator,
        mode,
        call,
        snt,
        rcv,
        comment,
        continent,
        modified,
        modifiedby
    FROM
        (SELECT qsos_raw.qsoid, MAX(modified) FROM qsos_raw GROUP BY qsoid) AS f
    INNER JOIN
        qsos_raw ON f.qsoid = qsos_raw.qsoid AND f.max = qsos_raw.modified
    WHERE call IS NOT NULL AND timestamp IS NOT NULL;

-- Function definitions for triggers of `qsos` view. See trigger definitions below for explanation.
CREATE FUNCTION insert_qso()
    RETURNS trigger AS
$BODY$
BEGIN
        INSERT INTO qsos_raw (qsoid, contestname, timestamp, band, rxfreq, txfreq, countryprefix, operator, mode, call, snt, rcv, comment, continent, modifiedby) VALUES (nextval('qsoid_generator'), NEW.contestname, NEW.timestamp, NEW.band, NEW.rxfreq, NEW.txfreq, NEW.countryprefix, NEW.operator, NEW.mode, NEW.call, NEW.snt, NEW.rcv, NEW.comment, NEW.continent, NEW.modifiedby);
   RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

CREATE FUNCTION delete_qso()
    RETURNS trigger AS
$BODY$
BEGIN
        INSERT INTO qsos_raw (qsoid, modified) VALUES (OLD.qsoid, current_timestamp);
   RETURN OLD;
END;
$BODY$ LANGUAGE plpgsql;

CREATE FUNCTION update_qso()
    RETURNS trigger AS
$BODY$
BEGIN
        INSERT INTO qsos_raw (qsoid, contestname, timestamp, band, rxfreq, txfreq, countryprefix, operator, mode, call, snt, rcv, comment, continent, modifiedby) VALUES (OLD.qsoid, NEW.contestname, NEW.timestamp, NEW.band, NEW.rxfreq, NEW.txfreq, NEW.countryprefix, NEW.operator, NEW.mode, NEW.call, NEW.snt, NEW.rcv, NEW.comment, NEW.continent, NEW.modifiedby);
   RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;
-- End function definitions for triggers.

-- Triggered when doing INSERT on view `qsos`. Ensures that adding a new QSO to
-- `qsos` acquires a new qsoid from the qsoid generator, sets correct modified
-- timestamp and adds a new entry to `qsos_raw`.
CREATE TRIGGER add_qso
    INSTEAD OF INSERT ON qsos
    FOR EACH ROW
        EXECUTE PROCEDURE insert_qso();

-- Triggered when doing UPDATE on view `qsos`. Ensures that modifying an
-- existing entry in `qsos` creates a new entry in the `qsos_raw` table with
-- the same qsoid, new modified timestamp.
CREATE TRIGGER update_qso
    INSTEAD OF UPDATE ON qsos
    FOR EACH ROW
        EXECUTE PROCEDURE update_qso();

-- Triggered when doing DELETE on view `qsos`. Ensures that deleting an entry
-- from `qsos` adds an empty entry to `qsos_raw` with more recent modified and
-- the same qsoid.
CREATE TRIGGER delete_qso
    INSTEAD OF DELETE ON qsos
    FOR EACH ROW
        EXECUTE PROCEDURE delete_qso();

-- View over deleted QSOs.
CREATE VIEW deleted_qsos AS
    SELECT f.qsoid
    FROM
        (SELECT qsos_raw.qsoid, MAX(modified) FROM qsos_raw GROUP BY qsoid) AS f
    INNER JOIN
        qsos_raw ON f.qsoid = qsos_raw.qsoid AND f.max = qsos_raw.modified
    WHERE call IS NULL AND timestamp IS NULL;
