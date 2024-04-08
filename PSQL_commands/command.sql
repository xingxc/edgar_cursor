-- ALTER TABLE nvda_accession_numbers ADD CONSTRAINT "accession_number_constraint" PRIMARY KEY "accessionNumber";
-- alter table nvda_accession_numbers add constraint nvda_accession_numbers_pkey primary key (accessionNumber);




SELECT * from nvda_accession_numbers;

-- docker exec -it postgres0 psql -U postgres -d test -f "/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/PSQL_commands/command.sql"