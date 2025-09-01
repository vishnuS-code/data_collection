# fullrole_based.py

import time
import os,re
import streamlit as st
import paramiko
from db import Fetch_data, RemoteFetchData
from config import config
import subprocess



class FullRollZipper:
    def __init__(self, ssh_client, db=None):
        self.ssh_client = ssh_client
        self.db = db
        self.remote_dest_dir = "/home/kniti/MegaUpload"
        self.remote_zip = None
        self.roll_number = None

    def _exec(self, cmd):
        stdin, stdout, stderr = self.ssh_client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        return exit_status, stdout.read().decode(), stderr.read().decode()

    def upload_to_onedrive(self, remote_path, mill_name=None, machine_name=None, silent=False, **kwargs):
        """
        Upload folder/file to OneDrive via remote bash script.
        Clean output: progress bar, ETA, final summary only (unless silent=False).
        """
        try:
            if not remote_path:
                if not silent:
                    st.error("❌ No remote path provided.")
                return False

            mill_name = mill_name or kwargs.get("mill_name")
            machine_name = machine_name or kwargs.get("machine_name")

            if not mill_name or not machine_name:
                if not silent:
                    st.error("❌ Mill name and Machine name required.")
                return False

            if "onedrive_folder" in kwargs and not silent:
                st.warning(f"⚠️ Ignoring legacy argument: onedrive_folder={kwargs['onedrive_folder']}")

            remote_script = "/home/kniti/upload_to_onedrive.sh"
            cmd = f"bash {remote_script} '{mill_name}' '{machine_name}' '{remote_path}'"

            stdin, stdout, stderr = self.ssh_client.exec_command(cmd)

            buffer = ""
            uploaded_files = 0
            total_files = None

            re_upload = re.compile(r"^⬆️ Uploading (.+) → (.+)$")
            re_total = re.compile(r"^TOTAL_FILES=(\d+)$")

            while True:
                if stdout.channel.recv_ready():
                    chunk = stdout.channel.recv(4096).decode("utf-8", errors="ignore")
                    buffer += chunk
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue

                        # Parse total files
                        total_match = re_total.search(line)
                        if total_match:
                            total_files = int(total_match.group(1))
                            continue

                        # Parse file uploads
                        upload_match = re_upload.match(line)
                        if upload_match:
                            uploaded_files += 1

                if stdout.channel.exit_status_ready():
                    break
                time.sleep(0.2)

            exit_status = stdout.channel.recv_exit_status()
            if exit_status == 0:
                if not silent:
                    st.success(
                        f"✅ Upload completed: /Backup/{mill_name}/{machine_name}\n"
                        f"📦 {uploaded_files} files"
                    )
                return True
            else:
                error_output = stderr.read().decode().strip()
                if not silent:
                    st.error(f"❌ Remote script failed: {error_output}")
                return False

        except Exception as e:
            if not silent:
                st.error(f"❌ Upload process failed: {e}")
            return False


    def handle_full_roll_zip(self, roll_path, rolls, selected_roll, data_type, mill_name, machine_name):
        st.header("📤 Direct Upload to OneDrive (No Zipping)")

        # ✅ Extract roll_name
        try:
            _, roll_name = selected_roll.split(" - ")
        except ValueError:
            st.error("⚠️ Invalid roll format. Expected 'number - name'.")
            return

        folder_to_upload = os.path.join(roll_path, roll_name)

        # --- 🔍 Count all files and total size BEFORE upload ---
        try:
            status, count_out, _ = self._exec(f"find '{folder_to_upload}' -type f | wc -l")
            status, size_out, _ = self._exec(f"du -sb '{folder_to_upload}' | cut -f1")

            total_files = int(count_out.strip()) if count_out.strip().isdigit() else 0
            total_bytes = int(size_out.strip()) if size_out.strip().isdigit() else 0

            size_gb = total_bytes / (1024 ** 3)
            size_mb = total_bytes / (1024 ** 2)

            if total_files > 0:
                st.info(f"📦 Total files: {total_files} | Total size: {size_gb:.2f} GB ({size_mb:.0f} MB)")
            else:
                st.warning("⚠️ No files found in this roll.")
                return
        except Exception as e:
            st.error(f"❌ Could not calculate files/size: {e}")
            return

        # --- 🚀 Upload button ---
        if st.button("Upload Directly"):

            # --- Special handling for MDD (only JSONs in defect/labels) ---
            if data_type == "MDD":
                try:
                    st.info("📂 Collecting JSON files from `defect/labels`...")

                    cmd_find_jsons = f"find '{folder_to_upload}' -type f -path '*/defect/labels/*.json'"
                    status, out, err = self._exec(cmd_find_jsons)
                    if status != 0:
                        st.error(f"❌ Failed to list JSON files: {err}")
                        return

                    json_files = out.strip().splitlines()
                    if not json_files:
                        st.warning("⚠️ No JSON files found in defect/labels.")
                        return

                    st.info(f"📄 Found {len(json_files)} JSON files. Starting upload...")

                    # Upload with progress bar
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    uploaded_bytes = 0
                    start_time = time.time()
                    total_json_bytes = sum(os.path.getsize(f) for f in json_files if os.path.exists(f))

                    for idx, jf in enumerate(json_files, 1):
                        fsize = os.path.getsize(jf) if os.path.exists(jf) else 0
                        self.upload_to_onedrive(jf, mill_name, machine_name, silent=True)

                        uploaded_bytes += fsize
                        elapsed = int(time.time() - start_time)
                        pct = uploaded_bytes / total_json_bytes if total_json_bytes > 0 else idx / len(json_files)

                        progress_bar.progress(min(1.0, pct))
                        status_text.text(
                            f"⬆️ {idx}/{len(json_files)} JSON files | "
                            f"{uploaded_bytes / (1024**2):.1f} MB uploaded | "
                            f"Elapsed: {elapsed}s"
                        )

                    st.success(f"🌐 Uploaded {len(json_files)} JSON files from defect/labels to OneDrive ✅")

                except Exception as e:
                    st.error(f"❌ Failed to upload JSON files: {e}")
                    return

            # --- Add program details if FDA ---
            if data_type == "FDA" and self.db:
                try:
                    selected_roll_obj = next(
                        (r for r in rolls if f"{r[0]} - {r[1]}" == selected_roll), None
                    )
                    if selected_roll_obj:
                        _, _, machineprgdtl_id = selected_roll_obj
                        details = self.db.fetch_machine_program_detail(machineprgdtl_id)
                        if details:
                            program_details_file = os.path.join(folder_to_upload, "program_details.txt")
                            program_details_dir = os.path.dirname(program_details_file)

                            # ✅ Ensure directory exists
                            self._exec(f"mkdir -p '{program_details_dir}' && chmod 755 '{program_details_dir}'")

                            # ✅ Build the file content
                            content = "\n".join([f"{k}: {v}" for k, v in details.items()])

                            # ✅ Write directly inside the storage unit
                            self._exec(f"echo '{content}' > '{program_details_file}'")

                            st.success(f"✅ Program details TXT written in storage: {program_details_file}")
                except Exception as e:
                    st.warning(f"⚠️ Could not write program details TXT: {e}")

            # --- Upload remaining files to OneDrive (for FDA or general upload) ---
            try:
                st.info(f"📤 Uploading `{roll_name}` → OneDrive...")

                progress_bar = st.progress(0)
                status_text = st.empty()
                uploaded_bytes = 0
                start_time = time.time()

                # Get all files
                status, file_list_out, _ = self._exec(f"find '{folder_to_upload}' -type f")
                files = file_list_out.strip().split("\n") if file_list_out else []

                for idx, fpath in enumerate(files, 1):
                    fsize = os.path.getsize(fpath) if os.path.exists(fpath) else 0

                    # Upload file
                    self.upload_to_onedrive(fpath, mill_name, machine_name, silent=True)

                    uploaded_bytes += fsize
                    elapsed = int(time.time() - start_time)
                    pct = uploaded_bytes / total_bytes if total_bytes > 0 else idx / len(files)

                    progress_bar.progress(min(1.0, pct))
                    status_text.text(
                        f"⬆️ {idx}/{len(files)} files | "
                        f"{uploaded_bytes / (1024**2):.1f} MB uploaded | "
                        f"Elapsed: {elapsed}s"
                    )

                st.success("🌐 Folder uploaded to OneDrive successfully ✅")

            except Exception as e:
                st.error(f"❌ Error during upload: {e}")
