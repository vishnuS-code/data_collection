#!/bin/bash
set -euo pipefail
# set -x  # Disabled verbose output for cleaner logs

##########################################
# CONFIG
##########################################
TENANT_ID="{tenant_id}"
CLIENT_ID="{Client_id}"
CLIENT_SECRET="{Client_Secret}"
DRIVE_ID="{Drive_id}"

MILL_NAME="$1"
MACHINE_NAME="$2"
LOCAL_FOLDER="$3"

echo "🔍 Checking required packages..."

REQUIRED_PACKAGES=(curl jq parallel python3)
MISSING=()

for pkg in "${REQUIRED_PACKAGES[@]}"; do
    if ! command -v "$pkg" >/dev/null 2>&1; then
        MISSING+=("$pkg")
    fi
done

if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo "📦 Installing missing packages: ${MISSING[*]}"
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update -y
        sudo apt-get install -y "${MISSING[@]}"
    elif command -v yum >/dev/null 2>&1; then
        sudo yum install -y "${MISSING[@]}"
    else
        echo "❌ Package manager not found (apt-get or yum required)."
        exit 1
    fi
else
    echo "✅ All required packages are already installed."
fi


##########################################
# BASE PATH DETECTION
##########################################
if [[ "$LOCAL_FOLDER" == /home/kniti/projects/knit-i/knitting-core/data* ]]; then
    BASE_DATA="/home/kniti/projects/knit-i/knitting-core/data"
elif [[ "$LOCAL_FOLDER" == /home/kniti/projects/knit-i/knitting-core/images* ]]; then
    BASE_DATA="/home/kniti/projects/knit-i/knitting-core/images"
else
    BASE_DATA=""
fi

##########################################
# LOG SETUP
##########################################
SAFE_LOG_DIR="$HOME/onedrive_upload_logs"
mkdir -p "$SAFE_LOG_DIR"
chmod 755 "$SAFE_LOG_DIR"

if [[ -d "$LOCAL_FOLDER" ]]; then
    LOG_FILE="$LOCAL_FOLDER/upload_$(date +'%Y%m%d_%H%M%S').log"
else
    LOG_FILE="$SAFE_LOG_DIR/upload_$(date +'%Y%m%d_%H%M%S').log"
fi

exec > >(tee -a "$LOG_FILE") 2>&1

echo "============================="
echo "📤 OneDrive Upload Started"
echo "Mill: $MILL_NAME"
echo "Machine: $MACHINE_NAME"
echo "Local Folder: $LOCAL_FOLDER"
echo "Log File: $LOG_FILE"
echo "============================="

##########################################
# PRE-FLIGHT CHECKS
##########################################
if ! command -v jq >/dev/null 2>&1; then
    echo "❌ jq not found. Install jq and rerun."
    exit 1
fi

if [[ ! -e "$LOCAL_FOLDER" ]]; then
    echo "❌ Path not found: $LOCAL_FOLDER"
    exit 1
fi

if [[ ! -r "$LOCAL_FOLDER" ]]; then
    echo "❌ Path not readable: $LOCAL_FOLDER"
    exit 1
fi

##########################################
# FUNCTIONS
##########################################
FAILED_UPLOADS=()

url_encode() {
    python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$1"
}

get_access_token() {
    >&2 echo "🔑 Fetching access token..."
    local token
    token=$(curl -s -X POST "https://login.microsoftonline.com/$TENANT_ID/oauth2/v2.0/token" \
        -d "client_id=$CLIENT_ID" \
        -d "scope=https://graph.microsoft.com/.default" \
        -d "client_secret=$CLIENT_SECRET" \
        -d "grant_type=client_credentials" | jq -r '.access_token')
    
    if [[ -z "$token" ]]; then
        >&2 echo "❌ Failed to fetch access token!"
        exit 1
    fi
    echo "$token"
}

ensure_folder() {
    local parent_path="$1"
    local folder_name="$2"
    local folder_id
    local enc_parent enc_name

    enc_parent=$(url_encode "$parent_path")
    enc_name=$(url_encode "$folder_name")

    echo "📂 Ensuring folder exists: $parent_path/$folder_name"
    folder_id=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
        "https://graph.microsoft.com/v1.0/drives/$DRIVE_ID/root:/$enc_parent/$enc_name" \
        | jq -r '.id // empty')

    if [[ -z "$folder_id" || "$folder_id" == "null" ]]; then
        echo "📂 Creating folder: $parent_path/$folder_name"
        folder_id=$(curl -s -X POST \
            -H "Authorization: Bearer $ACCESS_TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"name\": \"$folder_name\", \"folder\": {}, \"@microsoft.graph.conflictBehavior\": \"replace\"}" \
            "https://graph.microsoft.com/v1.0/drives/$DRIVE_ID/root:/$enc_parent:/children" \
            | jq -r '.id // empty')
    else
        echo "ℹ️ Folder exists: $parent_path/$folder_name"
    fi
    echo "$folder_id"
}

upload_file() {
    local file_path="$1"
    local remote_path="$2"
    local file_name
    file_name=$(basename "$file_path")
    local enc_path enc_name http_status

    enc_path=$(url_encode "$remote_path")
    enc_name=$(url_encode "$file_name")

    # Minimal output
    echo "⬆️ Uploading $file_name → $remote_path"

    http_status=$(curl -s -o /dev/null -w "%{http_code}" \
        -X PUT \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/octet-stream" \
        --data-binary @"$file_path" \
        "https://graph.microsoft.com/v1.0/drives/$DRIVE_ID/root:/$enc_path/$enc_name:/content")

    if [[ "$http_status" -ge 200 && "$http_status" -lt 300 ]]; then
        echo "✅ Uploaded $file_name successfully"
    else
        echo "❌ Failed to upload $file_name (HTTP $http_status)"
        FAILED_UPLOADS+=("$file_path")
    fi
}

##########################################
# MAIN SCRIPT
##########################################
ACCESS_TOKEN=$(get_access_token)
ROOT_PATH="$MILL_NAME/$MACHINE_NAME"

echo "🔹 Ensuring root folders..."
ensure_folder "" "$MILL_NAME"
ensure_folder "$MILL_NAME" "$MACHINE_NAME"

##########################################
# UPLOAD LOGIC
##########################################
FAILED_LOG=$(mktemp)
export ACCESS_TOKEN DRIVE_ID ROOT_PATH FAILED_LOG
export -f url_encode upload_file

# Calculate remote directory relative to base path
if [[ -n "$BASE_DATA" ]]; then
    REL_PATH="${LOCAL_FOLDER#$BASE_DATA/}"
    REMOTE_DIR="$ROOT_PATH/$(dirname "$REL_PATH")"
else
    REMOTE_DIR="$ROOT_PATH"
fi

# Single file
if [[ -f "$LOCAL_FOLDER" ]]; then
    set +x  # Disable verbose output
    upload_file "$LOCAL_FOLDER" "$REMOTE_DIR"

# Directory upload
elif [[ -d "$LOCAL_FOLDER" ]]; then
    cd "$LOCAL_FOLDER"
    if ! command -v parallel >/dev/null 2>&1; then
        echo "❌ GNU parallel not found. Install it: sudo apt install parallel -y"
        exit 1
    fi

    PARALLEL_JOBS=4  
    echo "🔹 Uploading files in directory $LOCAL_FOLDER in parallel (jobs=$PARALLEL_JOBS)..."
    set -x  # Enable verbose for debugging parallel uploads
    find . -type f | parallel -j $PARALLEL_JOBS --will-cite '
        remote_dir="$ROOT_PATH/$(dirname {})"
        upload_file "{}" "$remote_dir" || echo "{}" >> "$FAILED_LOG"
    '
else
    echo "❌ Unknown path type: $LOCAL_FOLDER"
    exit 1
fi

rm -f "$FAILED_LOG"
echo "============================="
echo "📤 OneDrive Upload Completed!"
