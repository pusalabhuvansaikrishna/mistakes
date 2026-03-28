# app.py
import streamlit as st
import zipfile
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

from streamlit import sidebar

from processor import process_single_file

st.set_page_config(
    page_title="CSV URL Processor",
    page_icon="📤",
    layout="wide",
    initial_sidebar_state='collapsed'
)

st.title("📤 CSV URL Processor")
st.markdown("**Multi-file Parallel Processor** with proper support for Indian Languages (Devanagari)")

# Sidebar Settings
st.sidebar.header("⚙️ Configuration", )

url_column = st.sidebar.text_input(
    "URL Column Name",
    value="URL",
    help="Exact name of the column containing URLs"
)

max_workers = st.sidebar.slider(
    "Parallel Workers",
    min_value=1,
    max_value=8,
    value=4
)

# Main Upload
uploaded_files = st.file_uploader(
    "Upload Multiple CSV Files",
    type=["csv"],
    accept_multiple_files=True,
    help="You can select multiple files at once"
)

if uploaded_files:
    st.success(f"✅ {len(uploaded_files)} file(s) uploaded")

    if st.button("🚀 Start Processing", type="primary", use_container_width=True):

        results = {}
        progress_bar = st.progress(0)
        status_text = st.empty()

        with st.spinner("Processing files in parallel..."):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_name = {
                    executor.submit(process_single_file, uploaded_file, url_column): uploaded_file.name
                    for uploaded_file in uploaded_files
                }

                completed = 0
                for future in as_completed(future_to_name):
                    original_name = future_to_name[future]
                    try:
                        output_name, output_bytes = future.result()
                        results[output_name] = output_bytes
                        completed += 1
                        progress_bar.progress(completed / len(uploaded_files))
                        status_text.success(f"✅ {original_name} → {output_name}")
                    except Exception as e:
                        completed += 1
                        progress_bar.progress(completed / len(uploaded_files))
                        status_text.error(f"❌ Failed {original_name}: {str(e)}")

        # ====================== CREATE ZIP ======================
        if results:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for filename, data in results.items():
                    zip_file.writestr(filename, data)

            zip_buffer.seek(0)

            st.success(f"🎉 Processing completed! {len(results)} output files generated.")

            # Download ZIP Button
            st.download_button(
                label="📥 Download All Output Files as ZIP",
                data=zip_buffer.getvalue(),
                file_name="processed_outputs.zip",
                mime="application/zip",
                use_container_width=True,
                type="primary"
            )

            # Show list of files
            with st.expander("📋 Generated Files"):
                for name in results.keys():
                    st.write(f"• {name}")
        else:
            st.warning("No output files were generated.")
else:
    st.info("👆 Upload your CSV files containing URL column to start.")
