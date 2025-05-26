import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
from openpyxl.styles import numbers

# Optional: import your own merge logic
from merge_excel import merge_excel_data  # Make sure merge_excel.py is in the same folder

st.set_page_config(page_title="📊 Excel Tools", layout="wide")
st.title("📊 Excel Tools Dashboard")

# ---------- Excel formatter ----------
def to_excel_bytes(df: pd.DataFrame, id_columns=None) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, na_rep="N/A")
        worksheet = writer.sheets["Sheet1"]

        # Format ID columns to prevent scientific notation
        if id_columns:
            for col_idx, col_name in enumerate(df.columns, 1):
                if col_name in id_columns:
                    for row_idx in range(2, len(df) + 2):
                        cell = worksheet.cell(row=row_idx, column=col_idx)
                        cell.number_format = "@"
                        if isinstance(cell.value, (int, float)):
                            cell.value = str(int(cell.value))
    return output.getvalue()

# ---------- UI Tabs ----------
tab1, tab2 = st.tabs(["📊 Combine Excel Files", "🧮 Get Today's Results (Beijing Time)"])

# ===============================
# Tab 1: Combine Excel Files
# ===============================
with tab1:
    st.header("📊 Combine Excel Files")
    uploaded_files = st.file_uploader(
        "Upload one or more Excel files (.xlsx)",
        type="xlsx",
        accept_multiple_files=True
    )

    if uploaded_files:
        try:
            dfs = [pd.read_excel(f) for f in uploaded_files]
            combined_df = pd.concat(dfs, ignore_index=True)

            st.success(f"✅ Successfully combined {len(uploaded_files)} files.")
            st.write(f"Total rows: {len(combined_df)}")
            st.dataframe(combined_df.head())

            # Apply formatting to ID and large-number columns
            id_columns = ['ID', 'User ID', 'Campaign ID', 'Ad ID', 'Phone', 'Account Number']
            large_number_columns = [
                col for col in combined_df.columns
                if combined_df[col].dtype in ['int64', 'float64']
                and pd.notna(combined_df[col].max())
                and abs(combined_df[col].max()) >= 1e10
            ]
            columns_to_format = list(set(id_columns + large_number_columns))

            excel_data = to_excel_bytes(combined_df, id_columns=columns_to_format)
            st.download_button(
                label="📥 Download Combined Excel File",
                data=excel_data,
                file_name="combined_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"❌ Failed to combine files: {e}")

# ===============================
# Tab 2: Merge by Date (Beijing)
# ===============================
with tab2:
    st.header("🧮 Get Full-Day Results (Beijing Time)")

    col1, col2 = st.columns(2)
    with col1:
        table1_file = st.file_uploader("📄 Upload Table 1 (e.g. 3PM to midnight)", type="xlsx", key="file1")
    with col2:
        combined_table_file = st.file_uploader("📄 Upload Combined Table (both periods)", type="xlsx", key="file2")

    date1, date2 = st.columns(2)
    with date1:
        first_date = st.date_input("📆 First date (e.g., 2025-05-24)", value=datetime(2025, 5, 24))
    with date2:
        second_date = st.date_input("📆 Second date (e.g., 2025-05-25)", value=datetime(2025, 5, 25))

    if st.button("🔁 Run Merge"):
        if table1_file and combined_table_file:
            try:
                # Read table1 to infer target columns
                table1_df = pd.read_excel(table1_file)
                target_col_names = ["投放花费", '应用设备激活数', '付费用户数(首日)', 'd0']
                target_col_indices = [list(table1_df.columns).index(x) for x in target_col_names if x in table1_df.columns]
                target_columns = sorted(list(set(list(range(12)) + target_col_indices)))

                result_df = merge_excel_data(
                    table1_file,
                    combined_table_file,
                    target_columns=target_columns,
                    first_date=str(first_date),
                    second_date=str(second_date),
                    perform_sanity_check=False
                )

                st.success("✅ Merge completed successfully!")
                st.write("Preview of merged data:")
                st.dataframe(result_df.head(10))

                output_filename = second_date.strftime("%m%d") + "_results.xlsx"
                excel_data = to_excel_bytes(result_df, id_columns=['渠道ID', 'Ad Group ID', 'Ad ID'])

                st.download_button(
                    "📥 Download Merged Excel File",
                    data=excel_data,
                    file_name=output_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"❌ Merge failed: {e}")
        else:
            st.warning("⚠️ Please upload both required files before merging.")
