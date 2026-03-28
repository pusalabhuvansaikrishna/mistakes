# app.py
import streamlit as st
import zipfile
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from processor import process_single_file

st.set_page_config(page_title="CSV URL Processor", page_icon="📤", layout="wide")

st.title("📤 CSV URL Processor")
st.markdown("Upload multiple CSV files → Process **in parallel** → Download all outputs as ZIP")

uploaded_files = st.file_uploader(
    "Drop multiple CSV files here",
    type=["csv"],
    accept_multiple_files=True,
    help="You can select and upload many files at once"
)

col1, col2 = st.columns([3, 1])
with col1:
    url_column = st.text_input("URL Column Name", value="URL")
with col2:
    max_workers = st.slider("Number of Parallel Workers", min_value=1, max_value=8, value=4)

if uploaded_files:
    st.success(f"✅ {len(uploaded_files)} file(s) uploaded")

    if st.button("🚀 Start Parallel Processing", type="primary", use_container_width=True):
        with st.spinner("Processing files in parallel..."):
            results = {}
            progress_bar = st.progress(0)
            status_text = st.empty()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_name = {
                    executor.submit(process_single_file, file, url_column): file.name
                    for file in uploaded_files
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
                        status_text.error(f"❌ {original_name} failed: {e}")

            if results:
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
                    for name, data in results.items():
                        zipf.writestr(name, data)

                zip_buffer.seek(0)

                st.success(f"🎉 Processing completed! {len(results)} output file(s) ready.")

                st.download_button(
                    label="📥 Download All Results as ZIP",
                    data=zip_buffer.getvalue(),
                    file_name="all_processed_outputs.zip",
                    mime="application/zip",
                    use_container_width=True,
                    type="primary"
                )
            else:
                st.error("No files were successfully processed.")
else:
    st.info("👆 Please upload one or more CSV files to begin.")

