import datetime
import subprocess
import streamlit as st
import time,traceback
import os
import re
import paramiko
from db import Fetch_data, RemoteFetchData

MDD_DIR = "/home/kniti/projects/knit-i/knitting-core/data"
FDA_DIR = "/home/kniti/projects/knit-i/knitting-core/images"
CLOUD_UPLOAD_DIR = "/home/kniti/MegaUpload"

fetcher = Fetch_data()


class DoffBasedZipHandler:
    def __init__(self, choice, roll_path, roll_number, roll_name,
                 selected_date, ssh_client, db, machineprgdtl_id,
                 mill_name=None, machine_name=None):
        self.choice = choice
        self.roll_path = roll_path
        self.roll_number = roll_number
        self.roll_name = roll_name
        self.selected_date = selected_date
        self.ssh_client = ssh_client
        self.db = db
        self.machineprgdtl_id = machineprgdtl_id
        self.mill_name = mill_name
        self.machine_name = machine_name



    # -----------------------
    # SSH Helpers
    # -----------------------
    def _exec(self, cmd):
        try:
            out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode().splitlines()
            return out, None
        except subprocess.CalledProcessError as e:
            return [], str(e.output.decode())
        except Exception as e:
            return [], str(e)

  
    def _write_remote_file(self, path, content):
        sftp = self.ssh_client.open_sftp()
        with sftp.file(path, "w") as f:
            f.write(content)
        sftp.close()

    # -----------------------
    # File Size Check
    # -----------------------
    def write_program_details(self, output_dir):
        """Fetch and write program details into a TXT file."""
        if not self.machineprgdtl_id:
            print("⚠️ No machineprgdtl_id available for this roll.")
            return False

        details = self.db.fetch_machine_program_detail(self.machineprgdtl_id)
        if not details:
            print("⚠️ No program details found.")
            return False

        try:
            txt_path = os.path.join(output_dir, f"{self.roll_name}_program_details.txt")
            with open(txt_path, "w", encoding="utf-8") as f:
                for key, val in details.items():
                    f.write(f"{key}: {val}\n")
            print(f"✅ Program details written: {txt_path}")
            return True
        except Exception as e:
            print(f"⚠️ Could not write program details TXT: {e}")
            return False
    # -----------------------
    # OneDrive Upload
    # -----------------------
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

    # -----------------------
    # FDA Workflow
    # -----------------------
    def handle_fda(self):
        if self.choice != "Doff-based Zip":
            return

        st.subheader("FDA Doff-based Zip")

        # -----------------------------
        # Base path for FDA
        # -----------------------------
        fda_base_path = "/home/kniti/projects/knit-i/knitting-core/images"
        selected_date_str = (
            self.selected_date.strftime("%Y-%m-%d")
            if isinstance(self.selected_date, (datetime.date, datetime.datetime))
            else str(self.selected_date)
        )
        date_folder = os.path.join(fda_base_path, str(self.roll_name), selected_date_str)
        st.text(f"📂 Using roll/date folder: {date_folder}")

        ssh_client = self.ssh_client  # Ensure SSH client is set

        # -----------------------------
        # Verify date folder exists
        # -----------------------------
        stdin, stdout, stderr = ssh_client.exec_command(
            f"ls {os.path.join(fda_base_path, str(self.roll_name))}"
        )
        available_dates = stdout.read().decode().splitlines()
        if selected_date_str not in available_dates:
            st.warning(f"The folder is not available: {selected_date_str}")
            return

        # -----------------------------
        # List cameras
        # -----------------------------
        stdin, stdout, stderr = ssh_client.exec_command(f"ls {date_folder}")
        cameras = stdout.read().decode().splitlines()
        if not cameras:
            st.warning("No cameras found")
            return

        selected_cameras = st.multiselect("Select Cameras", cameras)
        if not selected_cameras:
            return

        # -----------------------------
        # Collect all images
        # -----------------------------
        all_files = []
        for cam in selected_cameras:
            cam_path = os.path.join(date_folder, cam)
            stdin, stdout, stderr = ssh_client.exec_command(
                f"find {cam_path} -type f -name '*.jpg'"
            )
            files = stdout.read().decode().splitlines()
            all_files.extend(files)

        if not all_files:
            st.warning("No files found")
            return

        # -----------------------------
        # Doff range input
        # -----------------------------
        col1, col2 = st.columns(2)
        min_doff = col1.number_input("Enter Minimum Doff ID", min_value=0, step=1, value=0)
        max_doff = col2.number_input("Enter Maximum Doff ID", min_value=0, step=1, value=0)
        if min_doff > max_doff:
            st.warning("⚠️ Invalid range")
            return

        # -----------------------------
        # Filter files by Doff
        # -----------------------------
        def extract_doff(fname):
            try:
                return int(os.path.basename(fname).split("_")[3])
            except Exception:
                return None

        files_in_range = [
            f for f in all_files
            if (d := extract_doff(f)) is not None and min_doff <= d <= max_doff
        ]

        if not files_in_range:
            st.warning(f"No files found in range {min_doff}–{max_doff}")
            return

        st.success(f"✅ Found {len(files_in_range)} files in range {min_doff}–{max_doff}")
        
         # ✅ Calculate total size of filtered files
        total_bytes = 0
        for f in files_in_range:
            stdin, stdout, stderr = ssh_client.exec_command(f"stat -c %s '{f}' || true")
            size_out = stdout.read().decode().strip()
            if size_out.isdigit():
                total_bytes += int(size_out)

        size_mb = total_bytes / (1024 ** 2)
        size_gb = total_bytes / (1024 ** 3)

        st.success(
            f"✅ Found {len(files_in_range)} files "
            f"(~{size_mb:.2f} MB / {size_gb:.2f} GB) in range {min_doff}–{max_doff}"
        )

        # -----------------------------
        # ✅ Write program_details.txt in the same folder
        # -----------------------------
        try:
            if self.db and self.machineprgdtl_id:
                details = self.db.fetch_machine_program_detail(self.machineprgdtl_id)
                if details:
                    program_details_file = os.path.join(date_folder, "program_details.txt")

                    # Ensure directory exists and is writable
                    self._exec(f"mkdir -p '{date_folder}' && chmod 755 '{date_folder}'")

                    # Prepare file content safely
                    lines = [f"{k}: {v}" for k, v in details.items()]
                    content = "\n".join(lines).replace("'", "'\"'\"'")  # escape single quotes

                    # Write directly into storage unit
                    self._exec(f"echo '{content}' > '{program_details_file}'")

                    st.success(f"✅ Program details TXT written: {program_details_file}")
                else:
                    st.warning("⚠️ No program details found for this roll")
            else:
                st.warning("⚠️ Database not connected or no machineprgdtl_id")
        except Exception as e:
            st.warning(f"⚠️ Could not write program details TXT: {e}")

        # -----------------------------
        # Calculate total ETA
        # -----------------------------
        try:
            total_bytes = 0
            for f in files_in_range:
                stdin, stdout, stderr = ssh_client.exec_command(f"stat -c %s '{f}' || true")
                size_out = stdout.read().decode().strip()
                if size_out.isdigit():
                    total_bytes += int(size_out)

            upload_speed_bytes_per_sec = 5 * 1024 * 1024  # 5 MB/s
            eta_sec = total_bytes / upload_speed_bytes_per_sec
            eta_min = int(eta_sec // 60)
            eta_rem_sec = int(eta_sec % 60)
            st.info(
                f"⏳ Estimated total upload time: ~{eta_min}m {eta_rem_sec}s "
                f"at {upload_speed_bytes_per_sec/1024/1024:.1f} MB/s"
            )

        except Exception as e:
            st.warning(f"⚠️ Could not calculate ETA dynamically: {e}")
            
        # -----------------------------
# Upload button (clean, batched progress only)
# -----------------------------
        mill_name = getattr(self, "mill_name", "DefaultMill")
        machine_name = getattr(self, "machine_name", "DefaultMachine")

        if st.button("Start Upload to OneDrive"):
            total_files = len(files_in_range)
            progress_bar = st.progress(0)
            status_text = st.empty()
            start_time = time.time()

            for idx, fpath in enumerate(files_in_range, 1):
                # Upload without printing per-file messages
                self.upload_to_onedrive(
                    remote_path=fpath,
                    mill_name=mill_name,
                    machine_name=machine_name,
                    silent=True  # 👈 suppress per-file logs
                )

                elapsed = int(time.time() - start_time)
                progress_bar.progress(idx / total_files)
                status_text.text(
                    f"⬆️ Uploading... {idx}/{total_files} files | Elapsed: {elapsed}s"
                )

            # ✅ After upload completes
            total_elapsed = int(time.time() - start_time)
            progress_bar.progress(1.0)
            status_text.text(
                f"✅ Upload completed: {total_files} files uploaded in {total_elapsed}s"
            )


    # -----------------------
    # MDA Workflow
    # -----------------------
    def handle_mda(self):
        if self.choice != "Doff-based Zip":
            return

        st.subheader("MDA Doff-based Data Collection")

        base_path = self.roll_path
        selected_date_str = (
            self.selected_date.strftime("%Y-%m-%d")
            if isinstance(self.selected_date, (datetime.date, datetime.datetime))
            else str(self.selected_date)
        )
        date_folder = os.path.join(base_path, self.roll_name, selected_date_str)
        st.text(f"📂 Using roll/date folder: {date_folder}")

        ssh_client = self.ssh_client  # Ensure self.ssh_client is set

        # -----------------------------
        # Step 0: Verify date folder exists
        # -----------------------------
        stdin, stdout, stderr = ssh_client.exec_command(f"ls {os.path.join(base_path, self.roll_name)}")
        available_dates = stdout.read().decode().splitlines()
        if selected_date_str not in available_dates:
            st.warning(f"The folder is not available: {selected_date_str}")
            return

        # -----------------------------
        # Step 1: List cameras
        # -----------------------------
        stdin, stdout, stderr = ssh_client.exec_command(f"ls {date_folder}")
        cameras = stdout.read().decode().splitlines()
        if not cameras:
            st.warning("No cameras found")
            return

        selected_cameras = st.multiselect("Select Cameras", cameras)
        if not selected_cameras:
            return

        # -----------------------------
        # Step 2: Collect defect types
        # -----------------------------
        union_defects = set()
        cam_defect_paths = {}
        for cam in selected_cameras:
            defect_label_path = os.path.join(date_folder, cam, "defect", "labels")
            cam_defect_paths[cam] = defect_label_path

            stdin, stdout, stderr = ssh_client.exec_command(f"ls {defect_label_path}")
            defects = stdout.read().decode().splitlines()
            union_defects.update(defects)

        union_defects = sorted(union_defects)
        if not union_defects:
            st.warning("No defect labels found")
            return

        selected_defects = st.multiselect("Select Defect Types", union_defects)
        if not selected_defects:
            return

        # -----------------------------
        # Step 3: Doff range input
        # -----------------------------
        min_doff = st.number_input("Enter Minimum Doff ID", min_value=0, step=1, value=0)
        max_doff = st.number_input("Enter Maximum Doff ID", min_value=0, step=1, value=0)
        if min_doff > max_doff:
            st.warning("⚠️ Invalid range")
            return

        # -----------------------------
        # Step 4: Collect files
        # -----------------------------
        all_files = []
        for cam in selected_cameras:
            for defect in selected_defects:
                defect_path = os.path.join(cam_defect_paths[cam], defect)
                stdin, stdout, stderr = ssh_client.exec_command(f"ls {defect_path}")
                files = stdout.read().decode().splitlines()
                for f in files:
                    if f:
                        all_files.append(os.path.join(defect_path, f))

        def extract_doff(fname):
            parts = fname.split("_")
            if len(parts) < 4:
                return None
            try:
                return int(parts[3])
            except ValueError:
                return None

        # Debug: print all doff IDs
        all_doff_ids = [extract_doff(os.path.basename(f)) for f in all_files]

        # Filter files based on user input
        files_in_range = [
            f for f in all_files
            if (d := extract_doff(os.path.basename(f))) is not None
            and min_doff <= d <= max_doff
        ]




        if not files_in_range:
            st.warning(f"No files in range {min_doff}–{max_doff}")
            return

        st.success(f"✅ Found {len(files_in_range)} files in range {min_doff}–{max_doff}")

         # ✅ Calculate total size of filtered files
        total_bytes = 0
        for f in files_in_range:
            stdin, stdout, stderr = ssh_client.exec_command(f"stat -c %s '{f}' || true")
            size_out = stdout.read().decode().strip()
            if size_out.isdigit():
                total_bytes += int(size_out)

        size_mb = total_bytes / (1024 ** 2)
        size_gb = total_bytes / (1024 ** 3)

        st.success(
            f"✅ Found {len(files_in_range)} files "
            f"(~{size_mb:.2f} MB / {size_gb:.2f} GB) in range {min_doff}–{max_doff}"
        )
       # -----------------------------
# Step 5: Calculate ETA (before upload)
# -----------------------------
        try:
            num_files = len(files_in_range)
            st.info(f"📂 Total files to upload: {num_files}")

            # Get total bytes for all files
            total_bytes = 0
            for f in files_in_range:
                stdin, stdout, stderr = ssh_client.exec_command(f"stat -c %s '{f}' || true")
                size_out = stdout.read().decode().strip()
                if size_out.isdigit():
                    total_bytes += int(size_out)

            # Upload speed in bytes/sec (can be dynamic later)
            upload_speed_bytes_per_sec = 5 * 1024 * 1024  # 5 MB/s

            eta_sec = total_bytes / upload_speed_bytes_per_sec
            eta_min = int(eta_sec // 60)
            eta_rem_sec = int(eta_sec % 60)
            st.info(f"⏳ Estimated total upload time: ~{eta_min}m {eta_rem_sec}s at {upload_speed_bytes_per_sec/1024/1024:.1f} MB/s")

        except Exception as e:
            st.warning(f"⚠️ Could not calculate ETA dynamically: {e}")


# -----------------------------
# Upload button (clean, batched progress only)
# -----------------------------
        mill_name = getattr(self, "mill_name", "DefaultMill")
        machine_name = getattr(self, "machine_name", "DefaultMachine")

        if st.button("Start Upload to OneDrive"):
            total_files = len(files_in_range)
            progress_bar = st.progress(0)
            status_text = st.empty()
            start_time = time.time()

            for idx, fpath in enumerate(files_in_range, 1):
                # Upload without printing per-file messages
                self.upload_to_onedrive(
                    remote_path=fpath,
                    mill_name=mill_name,
                    machine_name=machine_name,
                    silent=True  # 👈 suppress per-file logs
                )

                elapsed = int(time.time() - start_time)
                progress_bar.progress(idx / total_files)
                status_text.text(
                    f"⬆️ Uploading... {idx}/{total_files} files | Elapsed: {elapsed}s"
                )

            # ✅ After upload completes
            total_elapsed = int(time.time() - start_time)
            progress_bar.progress(1.0)
            status_text.text(
                f"✅ Upload completed: {total_files} files uploaded in {total_elapsed}s"
            )

