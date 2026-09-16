# import_deps pulls each dependency's locals_without_parens, so the macros
# they define keep the call style they are written in: `field :name, :string`
# (ecto), `add :col, :string` (ecto_sql), `check all x <- gen` (stream_data).
# Without these, `mix format` adds parens to every one of them and the
# formatter disagrees with the whole codebase.
[
  import_deps: [:ecto, :ecto_sql, :stream_data],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]
