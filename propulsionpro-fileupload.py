import streamlit as st
import requests
import mimetypes
import os

st.set_page_config(page_title="S3 File Upload with Metadata", layout="centered")

st.title("📁 Upload File to S3 with Metadata")

# 1. File Upload
uploaded_file = st.file_uploader("Select a file to upload", type=None)

# 2. Metadata Inputs
st.subheader("Metadata Attributes")

# Dropdown Metadata
dropdown_metadata_options = {
    "document_type": ["Main Engine", "Aux Engine", "Boiler Engine", "FMECA"],
    "engine_make": ["YANMAR CO. LTD.","ATLAS COPCO CORP.","ATLAS COPCO WUXI COMPRESSOR LTD.","DALIAN COSCO KHI SHIP ENG. CO. LTD.",
                    "DONGHWA PNEUTEC CO. LTD.","JONGHAP PNEUTEC CO. LTD.","TANABE PNEUMATIC MACH. CO. LTD."],
    "engine_model": ["C370","STARTAIR LT7-30  460/60","STARTAIR LT5-30","H-73","AHW-60A","AHV-30","AHV-20","VLH-43","H-74"
                     "LT-22-30","L9","L 20","LT-55-30-KE","WP15L","WP33L"],
    "person": ["Anand", "Tarun", "Sasi", "Vijay", "Uttam", "Others"],
    "info_type": ["Manual", "Service_Letter", "FMECA"]
}

# Textbox Metadata with validation rules
textbox_metadata_config = {
    "vessel": {"type": "integer", "label": "Vessel ID (Integer)"}
}

name_mapping = {
    "document_type":"Document Type",
    "engine_make":"Make",
    "engine_model":"Model",
    "person":"Person",
    "info_type":"Information Type",
    "vessel":"Vessel"
}

# Collect Dropdown Metadata (with 'Other' option handling)
selected_metadata = {}
for key, values in dropdown_metadata_options.items():
    key = key.lower()
    key_display = name_mapping[key]
    values_with_other = values + ["Other"]

    selected_value = st.selectbox(f"{key_display}", values_with_other, key=key)

    if selected_value == "Other":
        custom_value = st.text_input(f"Enter custom {key}")
        if custom_value.strip():
            selected_metadata[key] = custom_value.strip()
        else:
            st.warning(f"Please enter a custom value for {key}.")
    else:
        selected_metadata[key] = selected_value

# Collect Textbox Metadata with validation
textbox_metadata_values = {}
validation_errors = {}

for key, config in textbox_metadata_config.items():
    key = key.lower()
    
    user_input = st.text_input(config['label'])
    if user_input:
        if config['type'] == "integer":
            if user_input.isdigit():
                textbox_metadata_values[key] = user_input
            else:
                validation_errors[key] = "Must be an integer number."
        elif config['type'] == "string":
            if len(user_input.strip()) > 0:
                textbox_metadata_values[key] = user_input.strip()
            else:
                validation_errors[key] = "Cannot be empty."
    else:
        validation_errors[key] = "This field is required."

# Merge all metadata values
combined_metadata = {**selected_metadata, **textbox_metadata_values}
#combined_metadata["source"] = "main"
# Upload Button Logic
upload_button_disabled = not uploaded_file or bool(validation_errors)

if st.button("Upload File", disabled=upload_button_disabled):
    # Check for validation errors again
    if validation_errors:
        for field, error_msg in validation_errors.items():
            st.error(f"{field}: {error_msg}")
    else:
        # Get file details
        file_name = uploaded_file.name
        file_type, _ = mimetypes.guess_type(file_name)
        file_type = file_type or 'application/octet-stream'
        combined_metadata["document_name"] = os.path.basename(file_name)
        st.info("Requesting pre-signed URL...")

        # Prepare payload
        payload = {
            "fileName": file_name,
            "fileType": file_type,
            "metadata": combined_metadata,
            "request_type":"file_upload",
            "source":"main",
            "info_type":combined_metadata["info_type"]
        }

        # Call backend API for presigned URL
        try:
            url = "https://9a9d6v9vte.execute-api.ap-south-1.amazonaws.com/propulsionpro-websocket-upload-uat"
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()

            presigned_url = data['signedUrl']
            transaction_id = data['transactionId']
            s3_key = data['s3Key']  # Make sure backend returns this
            #print(payload)

            st.success(f"Received pre-signed URL (Transaction ID: {transaction_id})")
            st.info("Uploading file to S3...")

            # Prepare headers with metadata
            #headers = {f"x-amz-meta-{k.lower()}": v for k, v in combined_metadata.items()}
            #headers['Content-Type'] = file_type
            headers = {'Content-Type': file_type}

            # Upload file to S3
            upload_response = requests.put(presigned_url, data=uploaded_file, headers=headers)
            upload_response.raise_for_status()

            st.success("File uploaded successfully to S3!")
            st.info("Applying tags to the file in S3...")
            #del combined_metadata["source"]
            combined_metadata["source_url"] = "s3://synergy-oe-propulsionpro/"+s3_key
            # Step 3: Tag the uploaded object via backend API
            tag_payload = {
                "transactionId": transaction_id,
                "s3Key": s3_key,
                "tags": combined_metadata,
                "request_type":"tag_file"
            }
            print(combined_metadata)
            tag_response = requests.post(url, json=tag_payload)
            tag_response.raise_for_status()

            st.success("Tags applied successfully to the S3 object!")


        except requests.exceptions.RequestException as e:
            st.error(f"Error: {e}")
