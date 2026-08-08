"""CLI smoke tests."""

from click.testing import CliRunner

from sqtseries.cli import main


class TestCliHelp:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "sqtseries" in result.output

    def test_version(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_start_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["run", "--help"])
        assert result.exit_code == 0

    def test_subcommand_smoke(self):
        runner = CliRunner()
        # commands that don't require an existing database
        for cmd in ["status", "ports"]:
            result = runner.invoke(main, [cmd])
            assert result.exit_code == 0, f"{cmd} failed: {result.output}"


class TestModuleInvocation:
    def test_python_m_sqtseries(self):
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-m", "sqtseries", "--help"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert "sqtseries" in result.stdout
