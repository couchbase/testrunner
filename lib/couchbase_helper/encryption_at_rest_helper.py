
"""Shared encryption-at-rest primitives."""

from lib.remote.remote_util import RemoteMachineShellConnection


class EncryptionAtRestHelper:
    ENCRYPTION_MAGIC_BYTES = "Couchbase Encrypted"

    def __init__(self, log):
        self.log = log

    def get_file_header_text(self, node, file_path, bytes_to_read=512):
        shell = RemoteMachineShellConnection(node, verbose=False)
        cmd = f"xxd -l {bytes_to_read} -p {file_path} 2>/dev/null"
        output, _ = shell.execute_command(cmd)

        if not output:
            ls_out, _ = shell.execute_command(f"ls -la {file_path} 2>&1")
            xxd_err_out, _ = shell.execute_command(f"xxd -l 64 {file_path} 2>&1")
            file_type_out, _ = shell.execute_command(f"file {file_path} 2>&1")
            shell.disconnect()
            ls_str = ' '.join(ls_out).strip() if ls_out else "no ls output"
            xxd_err_str = ' '.join(xxd_err_out).strip() if xxd_err_out else "(xxd also silent with stderr)"
            file_type_str = ' '.join(file_type_out).strip() if file_type_out else "unknown"
            diag = f"ls: [{ls_str}] | file: [{file_type_str}] | xxd -l 64 (with stderr): [{xxd_err_str}]"
            return False, "", f"No output from xxd. {diag}"

        shell.disconnect()
        hex_str = ''.join(output).strip()
        try:
            raw_bytes = bytes.fromhex(hex_str)
            actual = raw_bytes.decode('ascii', errors='replace')
            printable = ''.join(
                chr(b) if 32 <= b < 127 else f'\\x{b:02x}'
                for b in raw_bytes
            )
            return True, actual, printable
        except (ValueError, UnicodeDecodeError) as e:
            return False, "", f"hex parse error: {str(e)}"

    def verify_file_encryption_magic_bytes(self, node, file_path):
        header_read, actual, printable = self.get_file_header_text(
            node, file_path, bytes_to_read=64
        )
        if header_read:
            if self.ENCRYPTION_MAGIC_BYTES in actual:
                return True, self.ENCRYPTION_MAGIC_BYTES
            return False, printable
        return False, printable
