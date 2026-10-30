"""Fer net at-rest encryption for bank statement files."""

from __future__ import annotations

import os

from cryptography.fernet import Fernet


def encrypt_file_to_disk(plaintext_path: str, encrypted_path: str) -> None:
    """
    Encrypt a bank statement file and write ciphertext to *encrypted_path*.

    The Fernet key is loaded from ``FERNET_KEY``, used in memory, and
    explicitly deleted before this function returns.  The key is never
    written to disk (ADR-007).
    """
    key_bytes: bytes = os.environ["FERNET_KEY"].encode()
    if not key_bytes:
        msg = "FERNET_KEY environment variable is not set."
        raise EnvironmentError(msg)

    f: Fernet = Fernet(key_bytes)
    try:
        with open(plaintext_path, "rb") as infile:
            ciphertext: bytes = f.encrypt(infile.read())
        with open(encrypted_path, "wb") as outfile:
            outfile.write(ciphertext)
    finally:
        del key_bytes
        del f


def encrypt_bytes(plaintext: bytes) -> bytes:
    """
    Encrypt raw in-memory bytes and return the Fernet ciphertext.

    Used by upload endpoints to normalise raw (plaintext) uploads to the
    encrypted-at-rest contract before anything touches disk. Key hygiene
    matches the file helpers (ADR-007): loaded from ``FERNET_KEY``, used
    in memory, deleted before returning.
    """
    key_bytes: bytes = os.environ["FERNET_KEY"].encode()
    if not key_bytes:
        msg = "FERNET_KEY environment variable is not set."
        raise EnvironmentError(msg)

    f: Fernet = Fernet(key_bytes)
    try:
        return f.encrypt(plaintext)
    finally:
        del key_bytes
        del f


def decrypt_file_to_memory(encrypted_path: str) -> bytes:
    """
    Decrypt a bank statement file and return plaintext bytes.

    The decrypted content exists only in memory — it is never written
    to disk.  The Fernet key is deleted before this function returns.
    """
    key_bytes: bytes = os.environ["FERNET_KEY"].encode()
    if not key_bytes:
        msg = "FERNET_KEY environment variable is not set."
        raise EnvironmentError(msg)

    f: Fernet = Fernet(key_bytes)
    try:
        with open(encrypted_path, "rb") as infile:
            plaintext: bytes = f.decrypt(infile.read())
        return plaintext
    finally:
        del key_bytes
        del f