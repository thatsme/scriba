-- Runs once, on first initialization of an empty data volume.
--
-- scriba_test already exists at this point (POSTGRES_DB in
-- docker-compose.yml). This adds the second database the bank example
-- needs, so `mix bank.setup` has nothing left to create.
CREATE DATABASE bank_demo;
