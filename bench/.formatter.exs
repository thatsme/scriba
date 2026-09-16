# :ecto is only a transitive dependency here, so import_deps can name
# :ecto_sql alone — it carries the migration macros (`add`, `create`).
# Schema fields in this project are written with parens.
[
  import_deps: [:ecto_sql],
  inputs: ["{mix,.formatter}.exs", "{config,lib,priv}/**/*.{ex,exs}"]
]
