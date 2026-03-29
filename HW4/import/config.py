import paramiko
import logging

HOST = "129.159.221.67"
USERNAME = "dsa"
PASSWORD = "academy2026"
REMOTE = "/home/dsa/dataset"


def pipeline(path, extract):
    for attempt in range(3):
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=HOST, username=USERNAME, password=PASSWORD)

                with ssh.open_sftp() as sftp:
                    try:
                        sftp.stat(f"{REMOTE}/{path}")
                    except IOError:
                        logging.error("Remote file/folder does not exist.")
                        break

                    if not extract(sftp):
                        break
            break

        except Exception as e:
            logging.warning(f"Error on attempt {attempt + 1}: {e}")
            if attempt < 2:
                logging.info("Retrying...")
