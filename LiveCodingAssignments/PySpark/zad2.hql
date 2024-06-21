-- Inicjalizacja sesji Hive
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;

-- Logowanie
CREATE TABLE IF NOT EXISTS hive_logs (log_message STRING);
INSERT INTO TABLE hive_logs VALUES ('===== 2.1ab =====');
INSERT INTO TABLE hive_logs VALUES ('Reading files...');

-- Wczytanie pliku CSV
CREATE TABLE IF NOT EXISTS df_nasa (
    col1 STRING, -- Dostosuj kolumny do rzeczywistej struktury danych
    col2 STRING,
    col3 INT
);

LOAD DATA INPATH 'hdfs://localhost:8020/user/vagrant/kolos/csv_data/NASA.csv' OVERWRITE INTO TABLE df_nasa;

INSERT INTO TABLE hive_logs VALUES ('done :)');

-- Analiza danych
INSERT INTO TABLE hive_logs VALUES ('===== 2.1c =====');
INSERT INTO TABLE hive_logs VALUES ('wpisy w podziale na reclass (zliczone):');

-- Zliczanie wpisów dla kazdej reclass
INSERT INTO TABLE hive_logs
SELECT recclass, COUNT(*) AS count
FROM df_nasa
GROUP BY recclass;

INSERT INTO TABLE hive_logs VALUES ('ilosc wpisów dla poszczegolnyvh lat spelniajacych kryterium:');

-- Tworzenie tymczasowej tabeli
CREATE TEMPORARY TABLE nasa_table AS SELECT * FROM df_nasa;

-- Zliczanie wpisów dla poszczególnych lat
INSERT INTO TABLE hive_logs
SELECT year, COUNT(*) AS count
FROM nasa_table
WHERE fall = 'Fell'
GROUP BY year
ORDER BY year;

INSERT INTO TABLE hive_logs VALUES ('dwuwymiarowa siatka:');

-- Tworzenie dwuwymiarowej siatki
CREATE TABLE IF NOT EXISTS df_rounded AS
SELECT
    rounded_latitude,
    rounded_longitude,
    SUM(mass_g) AS total_mass
FROM (
    SELECT
        ROUND(latitude) AS rounded_latitude,
        ROUND(longitude) AS rounded_longitude,
        mass_g
    FROM df_nasa
) t
GROUP BY rounded_latitude, rounded_longitude;

-- Wyswietlanie wyników
INSERT INTO TABLE hive_logs SELECT * FROM df_rounded;
