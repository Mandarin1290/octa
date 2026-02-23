"""Offline unit tests for octa.support.ibkr_credentials.

No X11, no subprocess calls, no network.  Uses tmp_path for env file I/O.
Secrets are never printed; only variable names and string lengths appear.

Coverage
--------
TestEnvVarResolution
    - Primary OCTA_IBKR_USERNAME/PASSWORD → source "env"
    - IBKR_USERNAME/PASSWORD alias → source "env"
    - TWS_USERNAME/TWS_PASSWORD alias → source "env"
    - IBKR_USER + IBKR_PASS alias → source "env"
    - IBKR_USER + IBKR_PW alias → source "env"
    - Mixed: username from primary, password from alias

TestEnvFileParsing
    - Bare KEY=VALUE
    - Quoted values (double quotes, single quotes)
    - Optional 'export ' prefix
    - CRLF line endings
    - Comments and blank lines ignored
    - Whitespace trimmed from key and value

TestLoadCredentials
    - Env var present → no file I/O
    - Env var missing, file present → source "file"
    - CRLF env file loaded correctly
    - Alias vars in env file loaded
    - Missing everywhere → (None, None, "missing")
    - File absent → (None, None, "missing")
    - OCTA_IBKR_ENV_FILE override path respected
    - Explicit env_file= path arg
    - Partial: username in env, password in file → source "file"

TestChainIntegration
    - Simulates chain scenario: OCTA_IBKR_USERNAME/PASSWORD absent,
      IBKR_USERNAME/IBKR_PASSWORD present via env → resolves as "env"
    - Simulates chain scenario: nothing in env but ibkr.env file present → "file"
    - Simulates chain scenario: nothing anywhere → "missing"
"""
from __future__ import annotations

import pytest

from octa.support.ibkr_credentials import (
    _parse_env_file,
    load_credentials,
)


# ---------------------------------------------------------------------------
# TestEnvVarResolution — check priority order within env dict
# ---------------------------------------------------------------------------


class TestEnvVarResolution:
    def test_primary_octa_vars(self) -> None:
        env = {"OCTA_IBKR_USERNAME": "alice", "OCTA_IBKR_PASSWORD": "secret1"}
        u, p, src = load_credentials(env)
        assert u == "alice" and p == "secret1" and src == "env"

    def test_ibkr_alias(self) -> None:
        env = {"IBKR_USERNAME": "bob", "IBKR_PASSWORD": "secret2"}
        u, p, src = load_credentials(env)
        assert u == "bob" and p == "secret2" and src == "env"

    def test_tws_alias(self) -> None:
        env = {"TWS_USERNAME": "carol", "TWS_PASSWORD": "secret3"}
        u, p, src = load_credentials(env)
        assert u == "carol" and p == "secret3" and src == "env"

    def test_ibkr_user_pass_alias(self) -> None:
        env = {"IBKR_USER": "dave", "IBKR_PASS": "secret4"}
        u, p, src = load_credentials(env)
        assert u == "dave" and p == "secret4" and src == "env"

    def test_ibkr_user_pw_alias(self) -> None:
        env = {"IBKR_USER": "eve", "IBKR_PW": "secret5"}
        u, p, src = load_credentials(env)
        assert u == "eve" and p == "secret5" and src == "env"

    def test_octa_username_ibkr_password_mix(self) -> None:
        # Username from primary, password from alias
        env = {"OCTA_IBKR_USERNAME": "frank", "IBKR_PASSWORD": "secret6"}
        u, p, src = load_credentials(env)
        assert u == "frank" and p == "secret6" and src == "env"

    def test_primary_takes_priority_over_alias(self) -> None:
        env = {
            "OCTA_IBKR_USERNAME": "primary_user",
            "IBKR_USERNAME": "alias_user",
            "OCTA_IBKR_PASSWORD": "primary_pass",
            "IBKR_PASSWORD": "alias_pass",
        }
        u, p, src = load_credentials(env)
        assert u == "primary_user" and p == "primary_pass" and src == "env"

    def test_whitespace_trimmed_from_env_value(self) -> None:
        env = {"IBKR_USERNAME": "  grace  ", "IBKR_PASSWORD": "  pw7  "}
        u, p, src = load_credentials(env)
        assert u == "grace" and p == "pw7" and src == "env"


# ---------------------------------------------------------------------------
# TestEnvFileParsing — _parse_env_file unit tests
# ---------------------------------------------------------------------------


class TestEnvFileParsing:
    def test_bare_key_value(self) -> None:
        result = _parse_env_file("IBKR_USERNAME=alice\nIBKR_PASSWORD=secret\n")
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "secret"

    def test_double_quoted_value(self) -> None:
        result = _parse_env_file('IBKR_USERNAME="alice"\nIBKR_PASSWORD="my secret"\n')
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "my secret"

    def test_single_quoted_value(self) -> None:
        result = _parse_env_file("IBKR_USERNAME='alice'\nIBKR_PASSWORD='pw'\n")
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "pw"

    def test_export_prefix_stripped(self) -> None:
        result = _parse_env_file("export IBKR_USERNAME=alice\nexport IBKR_PASSWORD=pw\n")
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "pw"

    def test_export_prefix_with_quotes(self) -> None:
        result = _parse_env_file('export IBKR_USERNAME="alice"\nexport IBKR_PASSWORD="pw"\n')
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "pw"

    def test_crlf_line_endings(self) -> None:
        result = _parse_env_file("IBKR_USERNAME=alice\r\nIBKR_PASSWORD=pw\r\n")
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "pw"

    def test_comments_ignored(self) -> None:
        text = "# This is a comment\nIBKR_USERNAME=alice\n# another comment\nIBKR_PASSWORD=pw\n"
        result = _parse_env_file(text)
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "pw"
        assert "# This is a comment" not in result

    def test_blank_lines_ignored(self) -> None:
        result = _parse_env_file("\n\nIBKR_USERNAME=alice\n\nIBKR_PASSWORD=pw\n\n")
        assert result["IBKR_USERNAME"] == "alice"
        assert result["IBKR_PASSWORD"] == "pw"
        assert len(result) == 2

    def test_no_equals_line_skipped(self) -> None:
        result = _parse_env_file("SOME_LINE_WITHOUT_EQUALS\nIBKR_USERNAME=alice\n")
        assert "SOME_LINE_WITHOUT_EQUALS" not in result
        assert result["IBKR_USERNAME"] == "alice"

    def test_value_with_equals_sign_preserved(self) -> None:
        # Value itself contains '=' — only first '=' is the separator
        result = _parse_env_file("TOKEN=abc=def=ghi\n")
        assert result["TOKEN"] == "abc=def=ghi"

    def test_leading_whitespace_stripped(self) -> None:
        result = _parse_env_file("  IBKR_USERNAME  =  alice  \n")
        assert result["IBKR_USERNAME"] == "alice"

    def test_empty_string_returns_empty_dict(self) -> None:
        assert _parse_env_file("") == {}

    def test_only_comments_returns_empty_dict(self) -> None:
        assert _parse_env_file("# comment\n# another\n") == {}


# ---------------------------------------------------------------------------
# TestLoadCredentials — integration of the full load_credentials() function
# ---------------------------------------------------------------------------


class TestLoadCredentials:
    def test_returns_env_without_file_io(self) -> None:
        env = {"IBKR_USERNAME": "alice", "IBKR_PASSWORD": "pw1"}
        u, p, src = load_credentials(env)
        assert u == "alice" and p == "pw1" and src == "env"

    def test_loads_from_file_when_env_empty(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("IBKR_USERNAME=fileuser\nIBKR_PASSWORD=filepw\n", encoding="utf-8")
        u, p, src = load_credentials({}, env_file=env_file)
        assert u == "fileuser" and p == "filepw" and src == "file"

    def test_crlf_env_file_loaded(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_bytes(b"IBKR_USERNAME=crlfuser\r\nIBKR_PASSWORD=crlfpw\r\n")
        u, p, src = load_credentials({}, env_file=env_file)
        assert u == "crlfuser" and p == "crlfpw" and src == "file"

    def test_quoted_values_in_env_file(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text('IBKR_USERNAME="quoted_user"\nIBKR_PASSWORD=\'quoted_pw\'\n', encoding="utf-8")
        u, p, src = load_credentials({}, env_file=env_file)
        assert u == "quoted_user" and p == "quoted_pw" and src == "file"

    def test_alias_vars_in_env_file(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("TWS_USERNAME=twsuser\nTWS_PASSWORD=twspw\n", encoding="utf-8")
        u, p, src = load_credentials({}, env_file=env_file)
        assert u == "twsuser" and p == "twspw" and src == "file"

    def test_export_prefix_in_env_file(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("export IBKR_USERNAME=expuser\nexport IBKR_PASSWORD=exppw\n", encoding="utf-8")
        u, p, src = load_credentials({}, env_file=env_file)
        assert u == "expuser" and p == "exppw" and src == "file"

    def test_missing_returns_none_no_file(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        nonexistent = tmp_path / "no_such_file.env"
        u, p, src = load_credentials({}, env_file=nonexistent)
        assert u is None and p is None and src == "missing"

    def test_missing_returns_none_empty_env_no_file(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        nonexistent = tmp_path / "no_such_file.env"
        u, p, src = load_credentials({}, env_file=nonexistent)
        assert u is None and p is None and src == "missing"

    def test_env_file_partial_username_only(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("IBKR_USERNAME=onlyuser\n", encoding="utf-8")
        u, p, src = load_credentials({}, env_file=env_file)
        # Password missing → missing
        assert u is None and p is None and src == "missing"

    def test_env_file_partial_password_only(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("IBKR_PASSWORD=onlypw\n", encoding="utf-8")
        u, p, src = load_credentials({}, env_file=env_file)
        assert u is None and p is None and src == "missing"

    def test_username_in_env_password_in_file(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("IBKR_PASSWORD=filepw\n", encoding="utf-8")
        env = {"IBKR_USERNAME": "envuser"}
        u, p, src = load_credentials(env, env_file=env_file)
        assert u == "envuser" and p == "filepw" and src == "file"

    def test_octa_ibkr_env_file_override(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "custom.env"
        env_file.write_text("IBKR_USERNAME=customuser\nIBKR_PASSWORD=custompw\n", encoding="utf-8")
        env = {"OCTA_IBKR_ENV_FILE": str(env_file)}
        u, p, src = load_credentials(env)
        assert u == "customuser" and p == "custompw" and src == "file"

    def test_explicit_env_file_arg_takes_precedence(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "explicit.env"
        env_file.write_text("IBKR_USERNAME=explicituser\nIBKR_PASSWORD=explicitpw\n", encoding="utf-8")
        other = tmp_path / "other.env"
        other.write_text("IBKR_USERNAME=other\nIBKR_PASSWORD=other\n", encoding="utf-8")
        env = {"OCTA_IBKR_ENV_FILE": str(other)}
        u, p, src = load_credentials(env, env_file=env_file)
        assert u == "explicituser" and p == "explicitpw" and src == "file"


# ---------------------------------------------------------------------------
# TestChainIntegration — simulate the exact chain.py credential resolution
# ---------------------------------------------------------------------------


class TestChainIntegration:
    """These tests simulate what chain.py does at lines 1080-1096.

    The chain reads user/pw from root_env (= os.environ + YAML launch_env).
    user_env_name = "OCTA_IBKR_USERNAME", pass_env_name = "OCTA_IBKR_PASSWORD".

    If the primary vars are absent, _load_ibkr_credentials(root_env) is called.
    """

    def _resolve(
        self,
        root_env: dict[str, str],
        user_env_name: str = "OCTA_IBKR_USERNAME",
        pass_env_name: str = "OCTA_IBKR_PASSWORD",
        *,
        env_file: object = None,
    ) -> tuple[str, str, str]:
        """Reproduce the chain.py credential resolution logic."""
        from pathlib import Path as _Path

        credential_source = "env"
        user = str(root_env.get(user_env_name) or "")
        pw = str(root_env.get(pass_env_name) or "")
        if not user or not pw:
            kwargs: dict = {}
            if env_file is not None:
                kwargs["env_file"] = _Path(str(env_file))
            _u, _p, credential_source = load_credentials(root_env, **kwargs)
            if _u and _p:
                user, pw = _u, _p
            else:
                credential_source = "missing"
        return user, pw, credential_source

    def test_primary_env_vars_set(self) -> None:
        root_env = {
            "OCTA_IBKR_USERNAME": "octa_user",
            "OCTA_IBKR_PASSWORD": "octa_pw",
        }
        user, pw, src = self._resolve(root_env)
        assert user == "octa_user" and pw == "octa_pw" and src == "env"

    def test_ibkr_alias_in_env_resolves(self) -> None:
        # tws_e2e.sh passes IBKR_USERNAME/IBKR_PASSWORD into the env
        root_env = {
            "IBKR_USERNAME": "alias_user",
            "IBKR_PASSWORD": "alias_pw",
        }
        user, pw, src = self._resolve(root_env)
        assert user == "alias_user" and pw == "alias_pw" and src == "env"

    def test_file_fallback_when_env_empty(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("IBKR_USERNAME=fileuser\nIBKR_PASSWORD=filepw\n", encoding="utf-8")
        root_env: dict[str, str] = {}  # no creds in environment
        user, pw, src = self._resolve(root_env, env_file=env_file)
        assert user == "fileuser" and pw == "filepw" and src == "file"
        # Confirms: would NOT return MISSING_CREDENTIALS

    def test_nothing_anywhere_returns_missing(self, tmp_path: object) -> None:
        assert isinstance(tmp_path, __import__("pathlib").Path)
        nonexistent = tmp_path / "no_file.env"
        root_env: dict[str, str] = {}
        user, pw, src = self._resolve(root_env, env_file=nonexistent)
        assert user == "" and pw == "" and src == "missing"
        # Confirms: would return MISSING_CREDENTIALS (user or pw empty)

    def test_returns_lengths_not_values(self, tmp_path: object) -> None:
        """Demonstrate that only lengths (not values) are safe to log."""
        assert isinstance(tmp_path, __import__("pathlib").Path)
        env_file = tmp_path / "ibkr.env"
        env_file.write_text("IBKR_USERNAME=abc123\nIBKR_PASSWORD=xyz789\n", encoding="utf-8")
        root_env: dict[str, str] = {}
        user, pw, src = self._resolve(root_env, env_file=env_file)
        assert user == "abc123" and pw == "xyz789"
        # Safe logging pattern:
        safe_log = {"username_len": len(user), "password_len": len(pw), "source": src}
        assert safe_log == {"username_len": 6, "password_len": 6, "source": "file"}
        assert "abc123" not in str(safe_log)
        assert "xyz789" not in str(safe_log)
