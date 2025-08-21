# main.py

import datetime
import os
import streamlit as st
import paramiko
from db import Fetch_data,RemoteFetchData
from config import config

# -------------------- DB Fetch -------------------- #
fetcher = Fetch_data()

# -------------------- SSH Helpers -------------------- #
MDD_DIR = "/home/kniti/projects/knit-i/knitting-core/data"
FDA_DIR = "/home/kniti/projects/knit-i/knitting-core/images"




def handle_mda_doff_based_zip(choice, roll_path, roll_number, selected_date, ssh_client):
    """
    Handles the Doff-based Zip workflow:
    - Lets user pick cameras & defect types
    - Lets user set doff range
    - Filters matching files
    - Creates a zip on the remote storage
    """
    try:
        if choice != "Doff-based Zip":
            return


        st.subheader("Doff-based Data Collection")

        selected_date_str = str(selected_date).split(" ")[0]  # e.g. "2025-08-21"
        date_folder = os.path.join(roll_path, selected_date_str)

        # --- Verify date folder exists ---
        stdin, stdout, stderr = ssh_client.exec_command(f"ls {roll_path}")
        available_dates = stdout.read().decode().splitlines()

        if selected_date_str not in available_dates:
            st.warning(f"No folder for selected roll and date: {selected_date_str}")
            return

        # --- Step 1: List cameras (multi-select) ---
        stdin, stdout, stderr = ssh_client.exec_command(f"ls {date_folder}")
        cameras = stdout.read().decode().splitlines()

        if not cameras:
            st.warning("No cameras found inside this roll/date folder")
            return

        selected_cameras = st.multiselect("Select Cameras", cameras)

        if not selected_cameras:
            return

        # --- Step 2: Build union of defect types across cameras ---
        union_defects = set()
        cam_defect_paths = {}
        for cam in selected_cameras:
            defect_label_path = os.path.join(date_folder, cam, "defect", "labels")
            cam_defect_paths[cam] = defect_label_path
            stdin, stdout, stderr = ssh_client.exec_command(f"ls {defect_label_path}")
            defect_types = stdout.read().decode().splitlines()
            union_defects.update(defect_types)

        union_defects = sorted(union_defects)
        if not union_defects:
            st.warning("No defect labels found for the selected cameras")
            return

        selected_defects = st.multiselect("Select Defect Types", union_defects)

        if not selected_defects:
            return

        # --- Step 3: Ask for doff range ---
        st.subheader("Select Doff Range")
        min_doff_input = st.number_input("Enter Minimum Doff ID", min_value=0, step=1, value=0)
        max_doff_input = st.number_input("Enter Maximum Doff ID", min_value=0, step=1, value=0)

        if min_doff_input > max_doff_input:
            st.warning("⚠️ Minimum doff cannot be greater than maximum doff")
            return

        # --- Step 4: Collect all matching files ---
        all_files = []
        for cam in selected_cameras:
            for defect in selected_defects:
                defect_path = os.path.join(cam_defect_paths[cam], defect)
                stdin, stdout, stderr = ssh_client.exec_command(f"ls {defect_path}")
                files = stdout.read().decode().splitlines()
                for f in files:
                    if f:
                        all_files.append(os.path.join(defect_path, f))

        def extract_doff(filename: str):
            try:
                parts = filename.split("_")
                return int(parts[2])  # 3rd position
            except Exception:
                return None

        files_in_range = [
            f for f in all_files
            if (lambda d: d is not None and min_doff_input <= d <= max_doff_input)(
                extract_doff(os.path.basename(f))
            )
        ]

        if not files_in_range:
            st.warning(f"No files found in doff range {min_doff_input} – {max_doff_input}")
            return

        # --- Step 5: Zip creation ---
        custom_zip_name = st.text_input("Enter a name for the doff zip file (without extension):")

        if custom_zip_name and st.button("Zip IT"):
            remote_dest_dir = "/home/kniti/MegaUpload"
            remote_zip = f"{remote_dest_dir}/{custom_zip_name}.zip"

            # Ensure remote dir exists
            mkdir_cmd = f"mkdir -p {remote_dest_dir}"
            ssh_client.exec_command(mkdir_cmd)

            roll_parent = os.path.dirname(roll_path)

            rel_paths = [os.path.relpath(f, roll_parent) for f in files_in_range]

            # Prepare newline-separated list of files
            file_list = "\n".join(rel_paths)

            # Use `zip -@` to read from stdin
            zip_cmd = f"cd '{roll_parent}' && zip -@ '{remote_zip}'"

            stdin, stdout, stderr = ssh_client.exec_command(zip_cmd)
            stdin.write(file_list + "\n")
            stdin.channel.shutdown_write()  # signal EOF

            exit_status = stdout.channel.recv_exit_status()

            if exit_status != 0:
                st.error(f"Failed to create doff zip file: {stderr.read().decode()}")
            else:
                st.success(f"✅ Doff-based zip created at: {remote_zip}")
                st.write(f"File available in remote path: `{remote_zip}`")
    except Exception as e:
        st.error(f"Error in Doff-based Zip: {e}")


def connect_ssh(ip, username="supernova", password="Charlemagne@1", timeout=15):
    """
    Connect to a remote machine via Paramiko SSH.
    Returns ssh client if successful, else None.
    """
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=ip,
            username=username,
            password=password,
            timeout=timeout,
            look_for_keys=False,
            allow_agent=False
        )
        st.success(f"Successfully connected to {ip}")
        return client
    except Exception as e:
        st.error(f"Machine {ip} is offline or SSH connection failed: {e}")
        return None

def connect_storage_through_machine(machine_client, storage_ip, username="supernova", password="Charlemagne@1"):
    """
    Connect to storage unit through an already-connected machine (jump host).
    """
    try:
        transport = machine_client.get_transport()
        dest_addr = (storage_ip, 22)
        local_addr = ("127.0.0.1", 0)
        channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)

        storage_client = paramiko.SSHClient()
        storage_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        storage_client.connect(
            hostname=storage_ip,
            username=username,
            password=password,
            sock=channel,
            look_for_keys=False,
            allow_agent=False
        )
        st.success(f"Successfully connected to Storage Unit {storage_ip} via machine")
        return storage_client
    except Exception as e:
        st.error(f"Failed to connect to Storage Unit {storage_ip} via machine: {e}")
        return None


def handle_fda_doff_based_zip(choice, roll_path, roll_number, selected_date, ssh_client, db=None, machineprgdtl_id=None):
    try:
        if choice != "Doff-based Zip":
            return

        st.subheader("FDA Doff-based Zip")

        # --- Step 1: List cameras ---
        date_folder = os.path.join(roll_path, str(selected_date))
        stdin, stdout, stderr = ssh_client.exec_command(f"ls {date_folder}")
        cams = [c for c in stdout.read().decode().splitlines() if c.startswith("blackcam")]

        if not cams:
            st.error("❌ No blackcam cameras found")
            return

        cams_selected = st.multiselect("Select Camera(s)", cams, key="fda_cam_select")
        if not cams_selected:
            return

        # --- Step 2: Enter doff range ---
        col1, col2 = st.columns(2)
        with col1:
            start_doff = st.number_input("Enter Start Doff", min_value=0, step=1, key="fda_start_doff")
        with col2:
            end_doff = st.number_input("Enter End Doff", min_value=0, step=1, key="fda_end_doff")

        if start_doff > end_doff:
            st.warning("⚠️ Start doff must be <= End doff")
            return

        # --- Step 3: Collect matching files ---
        all_files = []

        for cam in cams_selected:
            defect_path = os.path.join(date_folder, cam)
            stdin, stdout, stderr = ssh_client.exec_command(f"find {defect_path} -type f -name '*.jpg'")
            files = stdout.read().decode().splitlines()

            for f in files:
                basename = os.path.basename(f)
                try:
                    doff = int(basename.split("_")[3])
                except Exception:
                    continue
                if start_doff <= doff <= end_doff:
                    all_files.append(f)

        if not all_files:
            st.warning(f"❌ No files found in doff range {start_doff}-{end_doff} for selected cameras")
            return
        else:
            st.info(f"✅ Found {len(all_files)} files in doff range {start_doff}-{end_doff}")

        # --- Step 4: Fetch machine program details if db and machineprgdtl_id provided ---
        program_details_file = None
        if db and machineprgdtl_id:
            details = db.fetch_machine_program_detail(machineprgdtl_id)
            if details:
                program_details_file = f"/tmp/program_details_{roll_number}.txt"
                sftp = ssh_client.open_sftp()
                with sftp.file(program_details_file, "w") as f:
                    for k, v in details.items():
                        f.write(f"{k}: {v}\n")
                sftp.close()
                all_files.append(program_details_file)  # add to zip

        # --- Step 5: Enter zip filename ---
        cam_names = "_".join(cams_selected)
        custom_zip_name = st.text_input("Enter zip filename (without extension):", key="fda_zip_name")

        if st.button("Zip IT", key="fda_zip_btn"):
            remote_dest_dir = "/home/kniti/MegaUpload"
            remote_zip = f"{remote_dest_dir}/{custom_zip_name}.zip"

            ssh_client.exec_command(f"mkdir -p {remote_dest_dir}")

            # --- Step 6: Write file list to remote temp file for zip ---
            temp_file = f"/tmp/fda_file_list_{roll_number}.txt"
            sftp = ssh_client.open_sftp()
            with sftp.file(temp_file, "w") as f:
                f.write("\n".join(all_files) + "\n")
            sftp.close()

            zip_cmd = f"zip -r {remote_zip} -@ < {temp_file}"

            with st.spinner(f"Creating zip of {len(all_files)} files..."):
                stdin, stdout, stderr = ssh_client.exec_command(zip_cmd)
                exit_status = stdout.channel.recv_exit_status()

            if exit_status != 0:
                st.error(f"❌ Failed to create zip: {stderr.read().decode()}")
            else:
                st.success(f"✅ FDA doff-based zip created at: {remote_zip}")

    except Exception as e:
        st.error(f"Error in FDA doff-based zip: {e}")


def select_mill_and_machine():
    try:
        selected_mill_info = None
        selected_machine_info = None

        mill_list = fetcher.fetch_mill_details()
        if not mill_list:
            st.error("No mills found in the database.")
            return selected_mill_info, selected_machine_info

        mill_map = {mill['mill_name']: mill['milldetails_id'] for mill in mill_list}
        selected_mill_name = st.selectbox("Select Mill", ["--Select--"] + list(mill_map.keys()))

        if selected_mill_name != "--Select--":
            selected_mill_id = mill_map[selected_mill_name]
            selected_mill_info = next(m for m in mill_list if m['milldetails_id'] == selected_mill_id)

            machine_list = fetcher.fetch_machine_details(selected_mill_id)
            if not machine_list:
                st.warning("No machines found for this mill.")
                return selected_mill_info, selected_machine_info

            machine_map = {m['machine_name']: m['machinedetail_id'] for m in machine_list}
            selected_machine_name = st.selectbox("Select Machine", ["--Select--"] + list(machine_map.keys()))

            if selected_machine_name != "--Select--":
                selected_machine_id = machine_map[selected_machine_name]
                selected_machine_info = next(m for m in machine_list if m['machinedetail_id'] == selected_machine_id)

        return selected_mill_info, selected_machine_info
    except Exception as e:
        st.error(f"Error selecting mill or machine: {e}")
        return None, None
# -------------------- Streamlit App -------------------- #

st.title("Data Collector Software")

# --- Step 1: Select Mill & Machine ---
mill_info, machine_info = select_mill_and_machine()

if machine_info:
    ip_address = machine_info.get("ip_address")
    st.write(f"Machine IP: {ip_address}")

    # Initialize session state holders
    st.session_state.setdefault("machine_ssh", None)
    st.session_state.setdefault("storage_ssh", None)
    st.session_state.setdefault("custom_zip_name", "")
    st.session_state.setdefault("selected_roll", "--Select--")
    st.session_state.setdefault("choice", "--Select--")


    # --- Step 2: Connect to Machine & Storage Unit ---
    if ip_address:
        if st.button(f"Connect to Machine {ip_address}") or st.session_state.machine_ssh:
            if not st.session_state.machine_ssh:
                st.session_state.machine_ssh = connect_ssh(ip_address)

                if st.session_state.machine_ssh:
                    # Fetch machine hostname
                    stdin, stdout, stderr = st.session_state.machine_ssh.exec_command("hostname")
                    hostname = stdout.read().decode().strip()
                    if hostname:
                        st.success(f"Connected to Machine: {hostname}")
                    else:
                        st.error("Failed to fetch machine hostname")

                    # Connect storage via machine
                    storage_ip = config.get("Core", "storage_ip", fallback="169.254.0.3")
                    st.session_state.storage_ssh = connect_storage_through_machine(
                        st.session_state.machine_ssh, storage_ip
                    )

                    if st.session_state.storage_ssh:
                        stdin, stdout, stderr = st.session_state.storage_ssh.exec_command("hostname")
                        storage_hostname = stdout.read().decode().strip()
                        if storage_hostname:
                            st.success(f"Connected to Storage Unit: {storage_hostname}")
                        else:
                            st.error("Failed to fetch storage hostname")

            # --- Step 3: Select Roll Date ---
            db = RemoteFetchData(ip_address)  # wrapper with fetch_roll_dates, fetch_rolls_by_date
            roll_dates = db.fetch_roll_dates()

            if roll_dates:
                selected_date = st.selectbox("Select Roll Start Date", ["--Select--"] + [str(d) for d in roll_dates])
            else:
                selected_date = "--Select--"
                st.warning("No roll dates found in database")

            if selected_date != "--Select--":
                # --- Step 4: Fetch Rolls for Selected Date ---
                rolls = db.fetch_rolls_by_date(selected_date)

                if rolls:
                    roll_options = [f"{r[0]} - {r[1]}" for r in rolls]
                    selected_roll = st.selectbox(
                        "Select Roll Number & Name",
                        ["--Select--"] + roll_options,
                        key="selected_roll"
                    )

                    if st.session_state.selected_roll != "--Select--":
                        roll_number, roll_name = selected_roll.split(" - ", 1)

                        # --- Step 5: Select Data Type ---
                        st.subheader("Select Data Type on Storage Unit")
                        data_type = st.radio("Choose Data Type:", ["MDD", "FDA"], index=0, key="data_type_radio")

                        remote_dir = MDD_DIR if data_type == "MDD" else FDA_DIR
                        roll_path = os.path.join(remote_dir, roll_name)

                        st.success(f"Selected Roll: {roll_number} ({roll_name})")
                        st.write(f"Remote Path: `{roll_path}`")

                        if st.session_state.storage_ssh:
                            stdin, stdout, stderr = st.session_state.storage_ssh.exec_command(f"ls {roll_path}")
                            files_in_roll = stdout.read().decode().splitlines()
                            if files_in_roll:
                                st.write(f"Files in Roll: {files_in_roll}")
                            else:
                                st.warning("No files found in this roll path")
                else:
                    st.warning("No rolls found for the selected date")
            if st.session_state.storage_ssh and st.session_state.selected_roll != "--Select--":
               choice = st.radio(
                    "Do you want to collect the full roll folder or defect-based data?",
                    ["--Select--", "Full Roll Folder Zip", "Doff-based Zip"],
                    index=0,
                    key="collection_mode"
                )
               st.session_state.choice = choice

            if st.session_state.storage_ssh and st.session_state.choice == "Full Roll Folder Zip":
                st.subheader("Full Roll Folder Zip")
                # Ask user for custom zip filename (persistent)
                custom_zip_name = st.text_input(
                    "Enter a name for the zip file (without extension):",
                    st.session_state.custom_zip_name,
                    key="zip_name_input"
                )

                if st.button("Zip IT"):
                    if not custom_zip_name.strip():
                        st.error("Please enter a valid name for the zip file.")
                    else:
                        # Save it back to session so it persists
                        st.session_state.custom_zip_name = custom_zip_name.strip()

                        # Remote destination directory (on storage unit)
                        remote_dest_dir = "/home/kniti/MegaUpload"
                        remote_zip = f"{remote_dest_dir}/{custom_zip_name}.zip"

                        # Ensure remote dir exists
                        mkdir_cmd = f"mkdir -p {remote_dest_dir}"
                        st.session_state.storage_ssh.exec_command(mkdir_cmd)

                        # --- FIX: run zip from the parent dir of the roll ---
                        # Example: if roll_path=/home/kniti/projects/knit-i/knitting-core/data/34
                        # we want cwd=/home/kniti/projects/knit-i/knitting-core/data
                        roll_parent = os.path.dirname(roll_path)
                        roll_name = os.path.basename(roll_path)

                        zip_cmd = f"cd {roll_parent} && zip -r {remote_zip} {roll_name}"

                        stdin, stdout, stderr = st.session_state.storage_ssh.exec_command(zip_cmd)
                        exit_status = stdout.channel.recv_exit_status()

                        if exit_status != 0:
                            st.error(f"Failed to create zip file on storage unit: {stderr.read().decode()}")
                        else:
                            st.success(f"✅ Remote zip created at: {remote_zip}")
            if st.session_state.storage_ssh and st.session_state.choice == "Doff-based Zip":
                if data_type == "MDD":
                    handle_mda_doff_based_zip(choice, roll_path, roll_number, selected_date, st.session_state.storage_ssh)
                else:  # FDA
                    handle_fda_doff_based_zip(choice, roll_path, roll_number, selected_date, st.session_state.storage_ssh)
