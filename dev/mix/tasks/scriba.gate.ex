defmodule Mix.Tasks.Scriba.Gate do
  @shortdoc "Runs the release gate and reports one pass/fail"

  @moduledoc """
  The release gate from `SCRIBA_ARCHITECTURE.md` §14, as a command.

      mix scriba.gate              # everything, before tagging
      mix scriba.gate --fast       # skip dialyzer and the test suites
      mix scriba.gate --published  # after publishing: check what users see

  It exists because the gate was a list a human read and applied selectively.
  Releases went out with a stale install snippet, a changelog that understated
  its own contents, and an option that was documented but never wired up —
  each time after someone (me) checked the list and reported it clean. A list
  you interpret is not a gate; a command that exits non-zero is.

  Not covered here, because no script can check it: whether every claim in
  every document is true. That still takes reading the documents against the
  code (§14), and it is the step most likely to be skipped, so the summary
  below names it explicitly rather than letting a green run imply it.
  """

  use Mix.Task

  @checks_fast [:version_consistency, :changelog_entry, :format, :compile, :credo, :docs]
  @checks_full @checks_fast ++ [:dialyzer, :tests, :hex_build]

  # After publishing, the only documentation that matters is the copy users
  # actually read — the one in the tarball, rendered on HexDocs. 0.2.0 was
  # correct in git and wrong on HexDocs for four hours.
  @checks_published [:hex_version, :published_install_snippet, :git_tag]

  @impl true
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: [fast: :boolean, published: :boolean])

    checks =
      cond do
        opts[:published] -> @checks_published
        opts[:fast] -> @checks_fast
        true -> @checks_full
      end

    Mix.shell().info("\nScriba release gate — #{version()}\n")

    results = Enum.map(checks, fn check -> {check, run_check(check)} end)

    report(results)

    if Enum.any?(results, fn {_, result} -> match?({:error, _}, result) end) do
      Mix.raise("release gate failed")
    end

    Mix.shell().info("""

    Still unchecked by anything mechanical: whether the documents tell the
    truth about the code. Audit them before tagging (§14).
    """)
  end

  ## Checks

  # The one that would have caught the 0.2.0 install snippet: mix.exs says
  # 0.2.0 while the README told readers to depend on "~> 0.1".
  defp run_check(:version_consistency) do
    [major, minor, _patch] = String.split(version(), ".")
    expected = expected_requirement()

    errors =
      []
      |> check_readme_requirement(expected)
      |> check_stale_version_mentions(major, minor)

    case errors do
      [] -> :ok
      errors -> {:error, Enum.join(errors, "\n    ")}
    end
  end

  defp run_check(:changelog_entry) do
    version = version()
    changelog = File.read!("CHANGELOG.md")

    cond do
      not String.contains?(changelog, "## [#{version}]") ->
        {:error, "CHANGELOG has no entry for #{version}"}

      # Only while preparing a release. Once this version is tagged, content
      # under [Unreleased] is work heading for the next one, and failing on it
      # would mean the gate cannot be run during ordinary development — which
      # is how a gate stops being run at all.
      not tagged?(version) and unreleased_has_content?(changelog) ->
        {:error, "CHANGELOG [Unreleased] is not empty — fold it into #{version} before tagging"}

      true ->
        :ok
    end
  end

  defp run_check(:format), do: cmd("mix", ["format", "--check-formatted"])
  defp run_check(:compile), do: cmd("mix", ["compile", "--force", "--warnings-as-errors"])
  defp run_check(:credo), do: cmd("mix", ["credo", "--strict"])
  defp run_check(:dialyzer), do: cmd("mix", ["dialyzer"])
  defp run_check(:tests), do: cmd("mix", ["test"])
  defp run_check(:hex_build), do: cmd("mix", ["hex.build"])

  defp run_check(:hex_version) do
    with {:ok, body} <- http_get("https://hex.pm/api/packages/scriba"),
         [_, latest] <- Regex.run(~r/"releases":\[\{"version":"([^"]+)"/, body) do
      if latest == version() do
        :ok
      else
        {:error, "hex.pm serves #{latest}, this checkout is #{version()} — publish, or you are reading a stale checkout"}
      end
    else
      _ -> {:error, "could not read the published version from hex.pm"}
    end
  end

  # The most-copied block in the README, checked where a user meets it.
  defp run_check(:published_install_snippet) do
    expected = expected_requirement()
    url = "https://hexdocs.pm/scriba/#{version()}/readme.html"

    case http_get(url) do
      {:ok, body} ->
        # HexDocs syntax-highlights code, so the snippet arrives split across
        # <span> tags: strip markup first, then unescape entities.
        text =
          body
          |> String.replace(~r/<[^>]*>/, "")
          |> String.replace("&quot;", "\"")
          |> String.replace("&gt;", ">")
          |> String.replace("&lt;", "<")
          |> String.replace("&amp;", "&")

        cond do
          String.contains?(text, "{:scriba, \"#{expected}\"}") ->
            :ok

          true ->
            found = Regex.run(~r/\{:scriba, "([^"]+)"\}/, text)
            {:error, "published docs show #{inspect(found && Enum.at(found, 1))}, expected #{expected}"}
        end

      _ ->
        {:error, "could not fetch #{url} — docs may still be building"}
    end
  end

  defp run_check(:git_tag) do
    case System.cmd("git", ["tag", "--list", "v#{version()}"], stderr_to_stdout: true) do
      {"", _} -> {:error, "no v#{version()} tag"}
      {_, 0} -> :ok
      {output, _} -> {:error, last_line(output)}
    end
  end

  defp run_check(:docs) do
    case System.cmd("mix", ["docs"], stderr_to_stdout: true) do
      {output, 0} ->
        case output |> String.split("\n") |> Enum.count(&String.contains?(&1, "warning:")) do
          0 -> :ok
          n -> {:error, "mix docs emitted #{n} warning(s)"}
        end

      {output, _} ->
        {:error, "mix docs failed: #{last_line(output)}"}
    end
  end

  ## Version consistency

  defp check_readme_requirement(errors, expected) do
    readme = File.read!("README.md")

    case Regex.run(~r/\{:scriba, "([^"]+)"\}/, readme) do
      [_, ^expected] ->
        errors

      [_, found] ->
        ["README install snippet says {:scriba, \"#{found}\"}, expected \"#{expected}\"" | errors]

      nil ->
        ["README has no {:scriba, \"...\"} install snippet to check" | errors]
    end
  end

  # Scope claims pinned to an old version: "supported in v0.1", "frozen for
  # v0.1". Those are sentences nobody revisited, and they are what makes a
  # maintained library read as abandoned.
  #
  # Deliberately narrow. A reference to a *future* version is a roadmap
  # statement, a reference to a past one in "shipped after v0.1" or "fixed in
  # 0.1.3" is history, and `docs/post-v0.1.md` is a filename — none of those
  # are stale, and a check that flags them gets ignored, which is worse than
  # not having it.
  @scope_phrase ~r/\b(?:in|for|frozen for|supported in|shipped in|available in|as of)\s+v?(\d+)\.(\d+)\b(?!\.\d)/i

  defp check_stale_version_mentions(errors, major, minor) do
    {current_major, current_minor} = {String.to_integer(major), String.to_integer(minor)}

    for file <- ~w(README.md MIGRATION.md REBUILDING.md SCRIBA_ARCHITECTURE.md),
        File.exists?(file),
        {line, index} <- file |> File.read!() |> String.split("\n") |> Enum.with_index(1),
        [_, found_major, found_minor] <- Regex.scan(@scope_phrase, line),
        older?({found_major, found_minor}, {current_major, current_minor}),
        reduce: errors do
      acc ->
        [
          "#{file}:#{index} pins scope to an older version: #{String.trim(line)}"
          | acc
        ]
    end
  end

  defp older?({found_major, found_minor}, {current_major, current_minor}) do
    {String.to_integer(found_major), String.to_integer(found_minor)} <
      {current_major, current_minor}
  end

  defp unreleased_has_content?(changelog) do
    case String.split(changelog, "## [Unreleased]", parts: 2) do
      [_, rest] ->
        rest
        |> String.split(~r/\n## \[/, parts: 2)
        |> hd()
        |> String.trim()
        |> Kernel.!=("")

      _ ->
        false
    end
  end

  ## Helpers

  defp cmd(command, args) do
    case System.cmd(command, args, stderr_to_stdout: true) do
      {_, 0} -> :ok
      {output, code} -> {:error, "#{command} #{Enum.join(args, " ")} exited #{code}: #{last_line(output)}"}
    end
  end

  defp last_line(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find("", &(String.trim(&1) != ""))
    |> String.slice(0, 200)
  end

  defp tagged?(version) do
    case System.cmd("git", ["tag", "--list", "v#{version}"], stderr_to_stdout: true) do
      {output, 0} -> String.trim(output) != ""
      _ -> false
    end
  end

  defp version, do: Mix.Project.config()[:version]

  defp expected_requirement do
    [major, minor, _patch] = String.split(version(), ".")
    "~> #{major}.#{minor}"
  end

  defp http_get(url) do
    case System.cmd("curl", ["-sL", url], stderr_to_stdout: true) do
      {body, 0} when byte_size(body) > 0 -> {:ok, body}
      _ -> :error
    end
  end

  defp report(results) do
    Enum.each(results, fn {check, result} ->
      case result do
        :ok ->
          Mix.shell().info("  ✓ #{check}")

        {:error, reason} ->
          Mix.shell().error("  ✗ #{check}\n    #{reason}")
      end
    end)
  end
end
