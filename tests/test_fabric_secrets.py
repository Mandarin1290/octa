import base64
import os

from octa_fabric.secrets import EncryptedFileBackend, EnvSecretsBackend


def test_env_secrets_backend(monkeypatch):
    monkeypatch.setenv("OCTA_SECRET_APIKEY", "s3cr3t")
    b = EnvSecretsBackend()
    assert b.get("APIKEY") == "s3cr3t"


def test_encrypted_file_backend(tmp_path, monkeypatch):
    # build a small secrets plaintext and encrypt using AESGCM
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except Exception:
        # cryptography not installed in environment; skip
        return

    key = AESGCM.generate_key(bit_length=128)
    aes = AESGCM(key)
    nonce = os.urandom(12)
    plain = b"APIKEY=safe\nOTHER=val\n"
    ct = aes.encrypt(nonce, plain, None)
    blob = nonce + ct
    f = tmp_path / "secrets.bin"
    f.write_bytes(blob)
    # provide key in base64
    monkeypatch.setenv("OCTA_SECRETS_KEY", base64.b64encode(key).decode())
    b = EncryptedFileBackend(path=str(f))
    assert b.get("APIKEY") == "safe"
