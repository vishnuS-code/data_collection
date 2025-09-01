# main.py
import time
import datetime
import os
import streamlit as st
import paramiko
from db import Fetch_data, RemoteFetchData
from doff_based import DoffBasedZipHandler
from fullrole_based import FullRollZipper
from config import config
import subprocess,traceback

fetcher = Fetch_data()


class MachineManager:
    def __init__(self):
        self.fetcher = Fetch_data()
        self.MDD_DIR = "/home/kniti/projects/knit-i/knitting-core/data"
        self.FDA_DIR = "/home/kniti/projects/knit-i/knitting-core/images"
        self.CLOUD_UPLOAD_DIR = "/home/kniti/MegaUpload"

        # Session state setup
        if "connected" not in st.session_state:
            st.session_state.connected = False
        if "machine_ssh" not in st.session_state:
            st.session_state.machine_ssh = None
        if "storage_ssh" not in st.session_state:
            st.session_state.storage_ssh = None

    def connect_ssh(self, ip, username="supernova", password="Charlemagne@1", timeout=15):
        """Connect to a remote machine via Paramiko SSH."""
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
            print(traceback.format_exc())
            return None

    def connect_storage_through_machine(self, machine_client, storage_ip, username="supernova", password="Charlemagne@1"):
        """Connect to storage unit through an already-connected machine (jump host)."""
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
            print(traceback.format_exc())
            return None

    def select_mill_and_machine(self):
        """Dropdowns for selecting Mill and Machine."""
        try:
            selected_mill_info = None
            selected_machine_info = None

            mill_list = self.fetcher.fetch_mill_details()
            if not mill_list:
                st.error("No mills found in the database.")
                return selected_mill_info, selected_machine_info

            mill_map = {mill['mill_name']: mill['milldetails_id'] for mill in mill_list}
            selected_mill_name = st.selectbox("Select Mill", ["--Select--"] + list(mill_map.keys()))

            if selected_mill_name != "--Select--":
                selected_mill_id = mill_map[selected_mill_name]
                selected_mill_info = next(m for m in mill_list if m['milldetails_id'] == selected_mill_id)

                machine_list = self.fetcher.fetch_machine_details(selected_mill_id)
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
            print(traceback.format_exc())
            return None, None

    def connect_to_machine_and_storage(self, machine_info):
        """SSH into machine and storage with proper validation and error handling."""
        import traceback
        import re

        ip_address = machine_info.get("ip_address")
        if not ip_address:
            st.error("❌ No machine IP provided in machine_info")
            return

        st.write(f"Machine IP: {ip_address}")

        if not st.session_state.get("connected", False):
            if st.button(f"Connect to Machine {ip_address}"):
                try:
                    # Connect to Machine
                    ssh = self.connect_ssh(ip_address)
                    if not ssh:
                        st.error(f"❌ Could not connect to machine at {ip_address}")
                        return

                    hostname = ssh.exec_command("hostname")[1].read().decode().strip()

                    # Fetch coreconfig.ini content using cat
                    stdin, stdout, stderr = ssh.exec_command("cat /home/kniti/projects/knit-i/config/coreconfig.ini")
                    config_text = stdout.read().decode().strip()
                    if not config_text:
                        st.error("❌ Could not read coreconfig.ini on machine")
                        return

                    # Extract storage_ip using regex
                    match = re.search(r"storage_ip\s*=\s*([0-9.]+)", config_text)
                    storage_ip = match.group(1) if match else None

                    if not storage_ip:
                        st.error("❌ No 'storage_ip' found in coreconfig.ini under [Core]")
                        return

                    st.write(f"📦 Storage IP from config: {storage_ip}")

                    # Connect to Storage through Machine
                    storage_ssh = self.connect_storage_through_machine(ssh, storage_ip)
                    if not storage_ssh:
                        st.error(f"❌ Failed to connect to Storage Unit at {storage_ip}")
                        return

                    storage_hostname = storage_ssh.exec_command("hostname")[1].read().decode().strip()

                    # Save sessions
                    st.session_state.machine_ssh = ssh
                    st.session_state.storage_ssh = storage_ssh
                    st.session_state.connected = True

                except Exception as e:
                    st.error(f"❌ Error during connection: {e}")
                    print(traceback.format_exc())
                    return

    def copy_upload_script(self):
        """Ensure upload_to_onedrive.sh exists on remote storage (always replace)."""
        local_script = os.path.join(os.path.dirname(__file__), "upload_to_onedrive.sh")
        remote_script = "/home/kniti/upload_to_onedrive.sh"

        if not os.path.exists(local_script):
            st.error(f"❌ Local script not found: {local_script}")
            return

        try:
            sftp = st.session_state.storage_ssh.open_sftp()
            try:
                sftp.remove(remote_script)
                st.info("ℹ️ Existing upload_to_onedrive.sh deleted")
            except FileNotFoundError:
                pass

            sftp.put(local_script, remote_script)
            sftp.close()

            stdin, stdout, stderr = st.session_state.storage_ssh.exec_command(f"chmod +x {remote_script}")
            exit_status = stdout.channel.recv_exit_status()

            if exit_status != 0:
                st.warning("⚠️ chmod failed, retrying with sudo...")
                stdin, stdout, stderr = st.session_state.storage_ssh.exec_command(f"sudo chmod +x {remote_script}")
                exit_status = stdout.channel.recv_exit_status()

                if exit_status != 0:
                    error_msg = stderr.read().decode().strip()
                    st.error(f"❌ Failed to set permissions even with sudo: {error_msg}")
                    return

            st.success("📂 upload_to_onedrive.sh copied and made executable")
        except Exception as e:
            st.error(f"❌ Failed to copy upload_to_onedrive.sh: {e}")


    def select_roll(self, ip_address):
        """Dropdown to select roll from DB by date."""
        db = RemoteFetchData(ip_address)
        selected_date = st.date_input(
            "Select Roll Start Date",
            value=None,
            min_value=datetime.date(2020, 1, 1),
            max_value=datetime.date.today()
        )

        rolls = db.fetch_rolls_by_date(selected_date) if selected_date else []
        if not rolls:
            st.warning("No rolls found for selected date")
            return None, None, None

        roll_options = [f"{r[0]} - {r[1]}" for r in rolls]
        selected_roll = st.selectbox("Select Roll Number & Name", ["--Select--"] + roll_options)

        return db, selected_date, selected_roll

# -------------------- Streamlit App -------------------- # 

def main():
    st.title("Data Collection Software")

    manager = MachineManager()

    # -------------------------------
    # Step 1: Select Mill & Machine
    # -------------------------------
    st.header("Step 1: Select Mill and Machine")
    mill_info, machine_info = manager.select_mill_and_machine()
    if not machine_info:
        st.stop()

    # -------------------------------
    # Step 2: Connect to Machine & Storage
    # -------------------------------
    st.header("Step 2: Connect")
    manager.connect_to_machine_and_storage(machine_info)
    if not st.session_state.get("connected", False):
        st.stop()

    # -------------------------------
    # Step 3: Ensure upload script
    # -------------------------------
    st.header("Step 3: Prepare Upload Script")
    manager.copy_upload_script()

    # -------------------------------
    # Step 4: Select Roll
    # -------------------------------
    st.header("Step 4: Select Roll")
    db, selected_date, selected_roll = manager.select_roll(machine_info["ip_address"])
    if not selected_roll:
        st.stop()

    st.success(f"✅ Roll selected: {selected_roll} ({selected_date})")
    time.sleep(1)

    # -------------------------------
    # Step 5: Choose Data Type & Zipping Method
    # -------------------------------
    if not selected_roll or selected_roll == "--Select--":
        st.warning("Please select a roll first.")
        st.stop()

# Safe to split now (must be outside the IF)
    roll_number, roll_name = selected_roll.split(" - ", 1)



    data_type = st.selectbox("Select Data Type", ["Select", "MDD", "FDA"])
    roll_path = None
    if data_type == "MDD":
        roll_path = manager.MDD_DIR
    elif data_type == "FDA":
        roll_path = manager.FDA_DIR

    st.header("Step 5: Choose Zipping Method")
    choice = st.radio("Select Zip Type", ["Select","Doff-based Zip", "Full Roll Zip"])

    # -------------------------------
    # Step 6: Handle Zipping
    # -------------------------------
    if data_type == "Select":
        st.warning("⚠️ Please choose either MDA or FDA to continue.")
        st.stop()

    if choice == "Doff-based Zip":
        st.subheader("📦 Doff-based Zipping")

        # Fetch rolls for the selected date
        rows = db.fetch_rolls_by_date(selected_date)

        # ✅ Resolve machineprgdtl_id from selected roll
        selected_roll_obj = next(
            (r for r in rows if f"{r[0]} - {r[1]}" == selected_roll),
            None
        )
        machineprgdtl_id = selected_roll_obj[2] if selected_roll_obj else None

        # ✅ Initialize handler (no selected_roll anymore)
        handler = DoffBasedZipHandler(
        choice=choice,
        roll_path=roll_path,
        roll_number=roll_number,
        roll_name=roll_name,
        selected_date=selected_date,
        ssh_client=st.session_state.storage_ssh,
        db=db,
        machineprgdtl_id=machineprgdtl_id,
        mill_name=mill_info.get("mill_name") if mill_info else None,
        machine_name=machine_info.get("machine_name") if machine_info else None
    )


        # Handle MDD / FDA separately
        if data_type == "MDD":
            handler.handle_mda()
        else:  # FDA
                handler.handle_fda()  # ✅ you can still pass selected_roll here



    elif choice == "Full Roll Zip":
        st.subheader("📦 Full Roll Zipping")
        zipper = FullRollZipper(st.session_state.storage_ssh, db=db)

        # Fetch dynamically from DB selection
        mill_name = mill_info.get("mill_name") if mill_info else None
        machine_name = machine_info.get("machine_name") if machine_info else None

        zipper.handle_full_roll_zip(
            roll_path=roll_path,
            rolls=db.fetch_rolls_by_date(selected_date),
            selected_roll=selected_roll,
            data_type=data_type,
            mill_name=mill_name,
            machine_name=machine_name
        )



if __name__ == "__main__":
    main()
