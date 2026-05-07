import Config

# Scriba is a library — no production config of its own. Only :test has
# environment-specific config (the real-Postgres test repo). :dev and :prod
# need no Scriba-side config at all.
case config_env() do
  :test -> import_config "test.exs"
  _ -> :ok
end
